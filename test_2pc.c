/*
 * Copyright (C) 2011 Colin Patrick McCabe <cmccabe@alumni.cmu.edu>
 *
 * This is licensed under the Apache License, Version 2.0.  See file COPYING.
 */

#define _GNU_SOURCE /* for TEMP_FAILURE_RETRY */

#include "worker.h"

#include <errno.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/************************ constants ********************************/
#define MAX_ACCEPTORS 128
#define DEFAULT_NUM_NODES 5

enum {
	MMM_DO_PROPOSE = 2000,
	MMM_PROPOSE = 3000,
	MMM_RESP,
	MMM_COMMIT_OR_ABORT,
};

enum node_state {
	NODE_STATE_INIT = 0,
	NODE_STATE_COORDINATING,
	NODE_STATE_COORDINATING_GOT_NACK,
	NODE_STATE_WAITING_FOR_COORDINATOR,
};

/************************ types ********************************/
struct node_data {
	/** node id */
	int id;
	/** current node state */
	enum node_state state;
	/** For each node, nonzero if we're waiting on a response from that 
	 * node */
	char waiting[MAX_ACCEPTORS];
	/** For each node, nonzero if we believe that node has failed */
	char failed[MAX_ACCEPTORS];
	/** In state NODE_STATE_WAITING_FOR_COORDINATOR, the current
	 * coordinator */
	int coord;
	/** who we believe is the current leader, or -1 if there is none */
	int leader;
};

struct mmm_do_propose {
	struct worker_msg base;
};

struct mmm_propose {
	struct worker_msg base;
	/** ID of sender */
	int src;
};

struct mmm_resp {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** 0 for NACK, 1 for ACK */
	int ack;
};

struct mmm_commit_or_abort {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** new leader, or -1 to abort */
	int leader;
};

/************************ globals ********************************/
/** acceptor nodes */
static struct worker *g_nodes[MAX_ACCEPTORS];

/** acceptor node data */
static struct node_data g_node_data[MAX_ACCEPTORS];

/** number of acceptors */
static int g_num_nodes;

static sem_t g_sem_accept_leader;

/************************ functions ********************************/
static void *xcalloc(size_t nmemb, size_t size)
{
	void *ret = calloc(nmemb, size);
	if (!ret)
		abort();
	return ret;
}

static int node_is_dead(struct node_data *me, int id)
{
	if (id < 0)
		return 1;
	return me->failed[id];
}

static int tpc_handle_msg(struct worker_msg *m, void *v)
{
	struct node_data *me = (struct node_data*)v;
	struct mmm_propose *mprop;
	struct mmm_resp *mresp;
	struct mmm_commit_or_abort *mca;
	int i;

	switch (m->ty) {
	case MMM_DO_PROPOSE:
		if ((me->state != NODE_STATE_INIT) ||
				!node_is_dead(me, me->leader)) {
			break;
		}
		memset(me->waiting, 0, sizeof(me->waiting));
		for (i = 0; i < g_num_nodes; ++i) {
			struct mmm_propose *m;
			
			if (i == me->id)
				continue;
			m = xcalloc(1, sizeof(struct mmm_propose));
			m->base.ty = MMM_PROPOSE;
			m->src = me->id;
			if (worker_sendmsg_or_free(g_nodes[i], m)) {
				fprintf(stderr, "%d: declaring node %d as failed\n", me->id, i);
				me->failed[i] = 1;
			}
			else {
				me->waiting[i] = 1;
			}
		}
		me->state = NODE_STATE_COORDINATING;
		break;
	case MMM_PROPOSE:
		mprop = (struct mmm_propose *)m;
		if ((!node_is_dead(me, me->leader)) ||
		    		(me->state != NODE_STATE_INIT)) {
			struct mmm_resp *m;

			m = xcalloc(1, sizeof(struct mmm_resp));
			m->base.ty = MMM_RESP;
			m->src = me->id;
			m->ack = 0;
			if (worker_sendmsg_or_free(g_nodes[mprop->src], m)) {
				fprintf(stderr, "%d: 2PC coordinator %d crashed: "
					"we're screwed\n", me->id, mprop->src);
				abort();
			}
		}
		else {
			struct mmm_resp *m;

			m = xcalloc(1, sizeof(struct mmm_resp));
			m->base.ty = MMM_RESP;
			m->src = me->id;
			m->ack = 1;
			if (worker_sendmsg_or_free(g_nodes[mprop->src], m)) {
				fprintf(stderr, "%d: 2PC coordinator %d crashed: "
					"we're screwed\n", me->id, mprop->src);
				abort();
			}
			me->state = NODE_STATE_WAITING_FOR_COORDINATOR;
			me->coord = mprop->src;
		}
		break;
	case MMM_RESP:
		if (!((me->state == NODE_STATE_COORDINATING) ||
				(me->state == NODE_STATE_COORDINATING_GOT_NACK))) {
			fprintf(stderr, "%d: got response message, but we are "
				"not coordinating. %d\n", me->id, m->ty);
			break;
		}
		mresp = (struct mmm_resp *)m;
		me->waiting[mresp->src] = 0;
		if (mresp->ack == 0)
			me->state = NODE_STATE_COORDINATING_GOT_NACK;
		for (i = 0; i < g_num_nodes; ++i) {
			if (me->waiting[i] != 0)
				break;
		}
		if (i != g_num_nodes) {
			/* still waiting for some responses */
			break;
		}
		/* send commit/abort message to all */
		for (i = 0; i < g_num_nodes; ++i) {
			struct mmm_commit_or_abort *m;
			
			if (i == me->id)
				continue;
			m = xcalloc(1, sizeof(struct mmm_commit_or_abort));
			m->base.ty = MMM_COMMIT_OR_ABORT;
			m->src = me->id;
			m->leader = (me->state == 
					NODE_STATE_COORDINATING_GOT_NACK) ? -1 : me->id;
			if (worker_sendmsg_or_free(g_nodes[i], m)) {
				fprintf(stderr, "%d: declaring node %d as failed\n", me->id, i);
				me->failed[i] = 1;
			}
			else {
				me->waiting[i] = 1;
			}
		}
		if (me->state == NODE_STATE_COORDINATING) {
			me->leader = me->id;
			sem_post(&g_sem_accept_leader);
		}
		break;
	case MMM_COMMIT_OR_ABORT:
		mca = (struct mmm_commit_or_abort*)m;
		if (me->state != NODE_STATE_WAITING_FOR_COORDINATOR) {
			fprintf(stderr, "%d: received MMM_COMMIT_OR_ABORT, but "
				"we are not waiting for a coordinator.\n", me->id);
			break;
		}
		if (me->coord != mca->src) {
			fprintf(stderr, "%d: received MMM_COMMIT_OR_ABORT, but "
				"we are waiting for node %d, not node %d\n",
				me->id, me->coord, mca->src);
			break;
		}
		if (mca->leader != -1) {
			me->leader = mca->leader;
			sem_post(&g_sem_accept_leader);
		}
		me->state = NODE_STATE_INIT;
		me->coord = -1;
		break;
	default:
		fprintf(stderr, "%d: invalid message type %d\n", me->id, m->ty);
		abort();
		break;
	}
	return 0;
}

static int send_do_propose(int node_id)
{
	struct mmm_do_propose *m;

	m = xcalloc(1, sizeof(struct mmm_do_propose));
	m->base.ty = MMM_DO_PROPOSE;
	return worker_sendmsg_or_free(g_nodes[node_id], m);
}

static int run_tpc(void)
{
	int i;

	if (sem_init(&g_sem_accept_leader, 0, 0))
		abort();
	memset(g_nodes, 0, sizeof(g_nodes));
	memset(g_node_data, 0, sizeof(g_node_data));
	for (i = 0; i < g_num_nodes;  ++i) {
		char name[WORKER_NAME_MAX];

		snprintf(name, WORKER_NAME_MAX, "node_%3d", i);
		g_node_data[i].id = i;
		g_node_data[i].leader = -1;
		g_node_data[i].coord = -1;
		g_nodes[i] = worker_start(name, tpc_handle_msg,
				NULL, &g_node_data[i]);
		if (!g_nodes[i]) {
			fprintf(stderr, "failed to allocate node %d\n", i);
			abort();
		}
	}
	if (g_num_nodes < 3)
		abort();
	if (send_do_propose(2))
		abort();
	for (i = 0; i < g_num_nodes; ++i) {
		TEMP_FAILURE_RETRY(sem_wait(&g_sem_accept_leader));
	}
	printf("successfully elected a leader.\n");
	for (i = 0; i < g_num_nodes; ++i) {
		worker_stop(g_nodes[i]);
	}
	for (i = 0; i < g_num_nodes; ++i) {
		worker_join(g_nodes[i]);
	}
	sem_destroy(&g_sem_accept_leader);
	return 0;
}

static void usage(const char *argv0, int retcode)
{
	fprintf(stderr, "%s: a consensus protocol demo.\n\
options:\n\
-h:                     this help message\n\
-n <num-nodes>          set the number of acceptors (default: %d)\n\
", argv0, DEFAULT_NUM_NODES);
	exit(retcode);
}

static void parse_argv(int argc, char **argv)
{
	int c;

	g_num_nodes = DEFAULT_NUM_NODES;
	while ((c = getopt(argc, argv, "hn:")) != -1) {
		switch (c) {
		case 'h':
			usage(argv[0], EXIT_SUCCESS);
			break;
		case 'n':
			g_num_nodes = atoi(optarg);
			break;
		default:
			fprintf(stderr, "failed to parse argument.\n\n");
			usage(argv[0], EXIT_FAILURE);
			break;
		}
	}
	if (g_num_nodes < 3) {
		fprintf(stderr, "num_nodes = %d, but we can't run with "
				"num_nodes < 3.\n\n", g_num_nodes);
		usage(argv[0], EXIT_FAILURE);
	}
}

int main(int argc, char **argv)
{
	int ret;

	parse_argv(argc, argv);

	ret = worker_init();
	if (ret) {
		fprintf(stderr, "failed to initialize the worker "
			"subsystem: error %d\n", ret);
		return EXIT_FAILURE;
	}
	ret = run_tpc();
	if (ret) {
		fprintf(stderr, "run_tpc failed with error code %d\n",
			ret);
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
