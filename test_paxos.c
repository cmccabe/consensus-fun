/*
 * Copyright (C) 2011 Colin Patrick McCabe <cmccabe@alumni.cmu.edu>
 *
 * This is licensed under the Apache License, Version 2.0.  See file COPYING.
 */

#define _GNU_SOURCE /* for TEMP_FAILURE_RETRY */

#include "limits.h"
#include "util.h"
#include "worker.h"

#include <errno.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/************************ constants ********************************/
enum {
	MMM_DO_PROPOSE = 2000,
	MMM_DO_TIMEOUT,
	MMM_PROPOSE = 3000,
	MMM_REJECT,
	MMM_ACCEPT,
	MMM_COMMIT,
	MMM_COMMIT_RESP,
};

enum node_state {
	NODE_STATE_INIT = 0,
	NODE_STATE_PROPOSING,
	NODE_STATE_COMMITTING,
};

enum remote_state {
	REMOTE_STATE_NO_RESP = 0,
	REMOTE_STATE_FAILED,
	REMOTE_STATE_ACCEPTED,
	REMOTE_STATE_REJECTED,
};

/************************ types ********************************/
struct node_data {
	/** node id */
	int id;
	/** current node state */
	enum node_state state;
	/** Perceived state of remote nodes */
	uint8_t remotes[MAX_ACCEPTORS];
	/** The highest paxos sequence number we've seen so far */
	uint64_t seen_pseq;
	/** In NODE_STATE_PROPOSING, the paxos sequence number we are currently
	 * proposing */
	uint64_t prop_pseq;
	/** The highest leader we've accept yet, or -1 if none has been proposed */
	int prop_leader;
	/** who we believe is the current leader, or -1 if there is none */
	int leader;
};

struct mmm_do_propose {
	struct worker_msg base;
};

struct mmm_do_timeout {
	struct worker_msg base;
	uint64_t prop_pseq;
};

struct mmm_propose {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** The value being proposed */
	int prop_leader;
	/** Paxos sequence ID of this propsal */
	uint64_t prop_pseq;
};

struct mmm_resp {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** Value of the highest-numbered proposal that this node has already
	 * accepted */
	int leader;
};

struct mmm_reject {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** Sequence number of the highest proposal we've seen */
	uint64_t seen_pseq;
};

struct mmm_accept {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** The leader that we are accepting */
	int prop_leader;
};

struct mmm_commit {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** The value to commit */
	int prop_leader;
	/** Paxos sequence ID of this propsal */
	uint64_t prop_pseq;
};

struct mmm_commit_resp {
	struct worker_msg base;
	/** ID of sender */
	int src;
	/** nonzero if this is an affirmative response */
	int ack;
	/** Paxos sequence ID of the commit propsal we're responding to */
	uint64_t prop_pseq;
};

/************************ globals ********************************/
/** enable debugging spew */ 
static int g_verbose;

/** acceptor nodes */
static struct worker *g_nodes[MAX_ACCEPTORS];

/** acceptor node data */
static struct node_data g_node_data[MAX_ACCEPTORS];

/** number of acceptors */
static int g_num_nodes;

static sem_t g_sem_accept_leader;

/** True if acceptors are running */
static int g_start;

/** Lock that protects g_start */
pthread_mutex_t g_start_lock;

/** Condition variable associated with g_start */
pthread_cond_t g_start_cond;

/************************ functions ********************************/
static const char *msg_type_to_str(int ty)
{
	switch (ty) {
	case MMM_DO_PROPOSE:
		return "MMM_DO_PROPOSE";
	case MMM_DO_TIMEOUT:
		return "MMM_DO_TIMEOUT";
	case MMM_PROPOSE:
		return "MMM_PROPOSE";
	case MMM_REJECT:
		return "MMM_REJECT";
	case MMM_ACCEPT:
		return "MMM_ACCEPT";
	case MMM_COMMIT:
		return "MMM_COMMIT";
	case MMM_COMMIT_RESP:
		return "MMM_COMMIT_RESP";
	default:
		return "(unknown)";
	}
}

static int node_is_dead(struct node_data *me, int id)
{
	if (id < 0)
		return 1;
	return me->remotes[id] == REMOTE_STATE_FAILED;
}

static uint64_t outbid(uint64_t pseq, int id)
{
	uint64_t tmp;

	tmp = pseq >> 8;
	tmp++;
	tmp <<= 8;
	if (id > 0xff)
		abort();
	return tmp | id;
}

static void reset_remotes(uint8_t *remotes)
{
	int i;

	for (i = 0; i < g_num_nodes; ++i) {
		switch (remotes[i]) {
		case REMOTE_STATE_ACCEPTED:
		case REMOTE_STATE_REJECTED:
			remotes[i] = REMOTE_STATE_NO_RESP;
		}
	}
}

static int check_acceptor_resp(const struct node_data *me, int ty, int src)
{
	if (!me->state != NODE_STATE_PROPOSING) {
		fprintf(stderr, "%d: got %s message from %d, but we are "
			"not proposing.\n",
			me->id, msg_type_to_str(ty), src);
		return 1;
	}
	if (me->remotes[src] != REMOTE_STATE_NO_RESP) {
		fprintf(stderr, "%d: got %s message from %d, but "
			"remote_state for that node is already %d\n",
			me->id, msg_type_to_str(ty),
			src, me->remotes[src]);
		return 1;
	}
	return 0;
}

static int have_majority_of_acceptors(const struct node_data *me)
{
	int i, num_acc;

	num_acc = 0;
	for (i = 0; i < g_num_nodes; ++i) {
		if (me->remotes[i] == REMOTE_STATE_ACCEPTED)
			num_acc++;
	}
	return (num_acc > (g_num_nodes / 2));
}

static void send_delayed_do_propose(struct node_data *me)
{
	struct mmm_do_propose *m;
	int delay;

	m = xcalloc(1, sizeof(struct mmm_do_propose));
	m->base.ty = MMM_DO_PROPOSE;
	delay = random() % 100;
	worker_sendmsg_deferred_ms(g_nodes[me->id], m, delay);
}

static void send_commits(struct node_data *me)
{
	int i;

	for (i = 0; i < g_num_nodes; ++i) {
		struct mmm_commit *m;

		if (i == me->id)
			continue;
		m = xcalloc(1, sizeof(struct mmm_commit));
		m->base.ty = MMM_COMMIT;
		m->src = me->id;
		m->prop_leader = me->prop_leader;
		m->prop_pseq = me->prop_pseq;
		if (worker_sendmsg_or_free(g_nodes[i], m)) {
			fprintf(stderr, "%d: declaring node %d as failed\n", me->id, i);
			me->remotes[i] = REMOTE_STATE_FAILED;
		}
		else {
			me->remotes[i] = REMOTE_STATE_NO_RESP;
		}
	}
}

static int check_commit_resp(const struct node_data *me,
			const struct mmm_commit_resp *mresp)
{
	if (me->state != NODE_STATE_COMMITTING) {
		if (g_verbose) {
			fprintf(stderr, "%d: got MMM_COMMIT_RESP from %d, but we "
				"are not commiting any more.\n",
				me->id, mresp->src);
		}
		return 1;
	}
	if (mresp->prop_pseq != me->prop_pseq) {
		if (g_verbose) {
			fprintf(stderr, "%d: got MMM_COMMIT_RESP from %d, but "
				"mresp->prop_pseq is %"PRId64 ", not %"PRId64"\n",
				me->id, mresp->src, mresp->prop_pseq, me->prop_pseq);
		}
		return 1;
	}
	return 0;
}

static void accept_leader(struct node_data *me, int leader)
{
	me->leader = leader;
	sem_post(&g_sem_accept_leader);
	me->state = NODE_STATE_INIT;
	if (g_verbose) {
		fprintf(stderr, "%d: accepted leader %d\n", me->id, leader);
	}
}

static int paxos_handle_msg(struct worker_msg *m, void *v)
{
	struct node_data *me = (struct node_data*)v;
	struct mmm_propose *mprop;
	struct mmm_do_timeout *mtimeo;
	struct mmm_accept *macc; 
	struct mmm_reject *mrej; 
	struct mmm_commit *mcom; 
	struct mmm_commit_resp *mresp;
	int i;

	pthread_mutex_lock(&g_start_lock);
	while (g_start == 0)
		pthread_cond_wait(&g_start_cond, &g_start_lock);
	pthread_mutex_unlock(&g_start_lock);

	switch (m->ty) {
	case MMM_DO_PROPOSE:
		if ((me->state != NODE_STATE_INIT) ||
				!node_is_dead(me, me->leader)) {
			if (g_verbose)
				fprintf(stderr, "%d: ignoring do_propose\n", me->id);
			break;
		}
		reset_remotes(me->remotes);
		me->prop_pseq  = me->seen_pseq = outbid(me->seen_pseq, me->id);
		for (i = 0; i < g_num_nodes; ++i) {
			struct mmm_propose *m;

			if (i == me->id)
				continue;
			m = xcalloc(1, sizeof(struct mmm_propose));
			m->base.ty = MMM_PROPOSE;
			m->src = me->id;
			m->prop_leader = me->id;
			m->prop_pseq = me->prop_pseq;
			if (worker_sendmsg_or_free(g_nodes[i], m)) {
				fprintf(stderr, "%d: declaring node %d as failed\n", me->id, i);
				me->remotes[i] = REMOTE_STATE_FAILED;
			}
			else {
				if (g_verbose)
					fprintf(stderr, "%d: sent MMM_PROPOSE to "
						"node %d\n", me->id, i);
				me->remotes[i] = REMOTE_STATE_NO_RESP;
			}
		}
		mtimeo = xcalloc(1, sizeof(struct mmm_do_timeout));
		mtimeo->base.ty = MMM_DO_TIMEOUT;
		mtimeo->prop_pseq = me->prop_pseq;
		worker_sendmsg_deferred_ms(g_nodes[me->id], mtimeo, 1000);
		me->state = NODE_STATE_PROPOSING;
		break;
	case MMM_DO_TIMEOUT:
		mtimeo = (struct mmm_do_timeout*)m;
		if ((mtimeo->prop_pseq != me->prop_pseq) ||
		    		(me->state != NODE_STATE_PROPOSING)) {
			if (g_verbose) {
				fprintf(stderr, "%d: ignoring timeout for defunct "
					"pseq %"PRId64"\n", me->id, mtimeo->prop_pseq);
			}
			break;
		}
		if (g_verbose) {
			fprintf(stderr, "%d: abaondoning proposal %"PRId64"\n",
				me->id, me->prop_pseq);
		}
		me->prop_pseq = -1;
		me->state = NODE_STATE_INIT;
		reset_remotes(me->remotes);
		/* Wait a bit, then try to propose ourselves as the
		 * leader again. */
		send_delayed_do_propose(me);
		break;
	case MMM_PROPOSE:
		mprop = (struct mmm_propose *)m;
		if ((!node_is_dead(me, me->leader)) || 
				    (mprop->prop_pseq <= me->seen_pseq)) {
			struct mmm_reject *mrej;

			mrej = xcalloc(1, sizeof(struct mmm_reject));
			mrej->base.ty = MMM_REJECT;
			mrej->src = me->id;
			mrej->seen_pseq = me->seen_pseq;
			if (worker_sendmsg_or_free(g_nodes[mprop->src], m)) {
				me->remotes[mprop->src] = REMOTE_STATE_FAILED;
			}
			if (g_verbose)
				fprintf(stderr, "%d: rejected proposal from %d\n",
					me->id, mprop->src);
		}
		else {
			if (mprop->prop_leader > me->prop_leader)
				me->prop_leader = mprop->prop_leader;
			me->seen_pseq = mprop->prop_pseq;
			macc = xcalloc(1, sizeof(struct mmm_accept));
			macc->base.ty = MMM_ACCEPT;
			macc->src = me->id;
			macc->prop_leader = me->prop_leader;
			if (worker_sendmsg_or_free(g_nodes[mprop->src], m)) {
				me->remotes[mprop->src] = REMOTE_STATE_FAILED;
			}
		}
		break;
	case MMM_ACCEPT:
		macc = (struct mmm_accept *)m;
		if (check_acceptor_resp(me, MMM_ACCEPT, macc->src))
			break;
		if (g_verbose) {
			fprintf(stderr, "%d: got MMM_ACCEPT (prop_leader=%d) from %d\n",
				me->id, macc->prop_leader, macc->src);
		}
		me->remotes[macc->src] = REMOTE_STATE_ACCEPTED;
		if (!have_majority_of_acceptors(me))
			break;
		send_commits(me);
		me->state = NODE_STATE_COMMITTING;
		break;
	case MMM_REJECT:
		mrej = (struct mmm_reject *)m;
		if (check_acceptor_resp(me, MMM_REJECT, mrej->src))
			break;
		if (g_verbose) {
			fprintf(stderr, "%d: got MMM_REJECT (seen_pseq=%"
				PRId64") from %d\n", me->id, mrej->seen_pseq, mrej->src);
		}
		me->remotes[mrej->src] = REMOTE_STATE_REJECTED;
		break;
	case MMM_COMMIT:
		mcom = (struct mmm_commit*)m;
		mresp = xcalloc(1, sizeof(struct mmm_commit));
		mresp->base.ty = MMM_COMMIT_RESP;
		mresp->src = me->id;
		mresp->prop_pseq = mcom->prop_pseq;
		if ((me->state != NODE_STATE_INIT) ||
			    (mcom->prop_leader != me->prop_leader) ||
				(mcom->prop_pseq < me->prop_pseq)) {
			mresp->ack = 0;
		}
		else {
			mresp->ack = 1;
			accept_leader(me, mcom->prop_leader);
		}
		if (worker_sendmsg_or_free(g_nodes[mcom->src], mresp)) {
			me->remotes[mcom->src] = REMOTE_STATE_FAILED;
		}
		break;
	case MMM_COMMIT_RESP:
		mresp = (struct mmm_commit_resp *)m;
		if (check_commit_resp(me, mresp))
			break;
		if (!mresp->ack) {
			me->remotes[mresp->src] = REMOTE_STATE_REJECTED;
			break;
		}
		me->remotes[mresp->src] = REMOTE_STATE_ACCEPTED;
		if (!have_majority_of_acceptors(me))
			break;
		accept_leader(me, me->prop_leader);
		if (g_verbose)
			fprintf(stderr, "%d: considering protocol "
				"terminated\n", me->id);
		break;
	default:
		fprintf(stderr, "%d: invalid message type %d\n", me->id, m->ty);
		abort();
		break;
	}
	return 0;
}

static void send_do_propose(int node_id)
{
	struct mmm_do_propose *m;

	m = xcalloc(1, sizeof(struct mmm_do_propose));
	m->base.ty = MMM_DO_PROPOSE;
	worker_sendmsg_or_free(g_nodes[node_id], m);
	//return worker_sendmsg_or_free(g_nodes[node_id], m);
}

static int check_leaders(void)
{
	int i, leader, conflict;

	leader = -1;
	conflict = 0;
	for (i = 0; i < g_num_nodes; ++i) {
		if (leader == -1)
			leader = g_node_data[i].leader;
		else {
			if (leader != g_node_data[i].leader) {
				conflict = 1;
			}
		}
	}
	if (!conflict) {
		printf("successfully elected node %d as leader.\n", leader);
		return 0;
	}
	for (i = 0; i < g_num_nodes; ++i) {
		fprintf(stderr, "%d: leader = %d\n", i, g_node_data[i].leader);
	}
	return 1;
}

static int run_paxos(int duelling_proposers)
{
	int i;

	if (sem_init(&g_sem_accept_leader, 0, 0))
		abort();
	if (pthread_mutex_init(&g_start_lock, 0))
		abort();
	if (pthread_cond_init(&g_start_cond, 0))
		abort();
	g_start = 0;
	memset(g_nodes, 0, sizeof(g_nodes));
	memset(g_node_data, 0, sizeof(g_node_data));
	for (i = 0; i < g_num_nodes;  ++i) {
		char name[WORKER_NAME_MAX];

		snprintf(name, WORKER_NAME_MAX, "node_%3d", i);
		g_node_data[i].id = i;
		g_node_data[i].leader = -1;
		g_node_data[i].prop_leader = -1;
		g_nodes[i] = worker_start(name, paxos_handle_msg,
				NULL, &g_node_data[i]);
		if (!g_nodes[i]) {
			fprintf(stderr, "failed to allocate node %d\n", i);
			abort();
		}
	}
	if (g_num_nodes < 3)
		abort();
	send_do_propose(2);
	if (duelling_proposers) {
		send_do_propose(0);
		send_do_propose(1);
	}
	/* start acceptors */
	pthread_mutex_lock(&g_start_lock);
	g_start = 1;
	pthread_cond_broadcast(&g_start_cond);
	pthread_mutex_unlock(&g_start_lock);
	/* wait for consensus */
	for (i = 0; i < g_num_nodes; ++i) {
		TEMP_FAILURE_RETRY(sem_wait(&g_sem_accept_leader));
	}
	/* cleanup */
	for (i = 0; i < g_num_nodes; ++i) {
		worker_stop(g_nodes[i]);
	}
	for (i = 0; i < g_num_nodes; ++i) {
		worker_join(g_nodes[i]);
	}
	pthread_cond_destroy(&g_start_cond);
	g_start = 0;
	pthread_mutex_destroy(&g_start_lock);
	sem_destroy(&g_sem_accept_leader);

	return check_leaders();
}

static void usage(const char *argv0, int retcode)
{
	fprintf(stderr, "%s: a consensus protocol demo.\n\
options:\n\
-h:                     this help message\n\
-n <num-nodes>          set the number of acceptors (default: %d)\n\
-v                      enable verbose debug messages\n\
", argv0, DEFAULT_NUM_NODES);
	exit(retcode);
}

static void parse_argv(int argc, char **argv)
{
	int c;

	g_num_nodes = DEFAULT_NUM_NODES;
	g_verbose = 0;

	while ((c = getopt(argc, argv, "hn:v")) != -1) {
		switch (c) {
		case 'h':
			usage(argv[0], EXIT_SUCCESS);
			break;
		case 'n':
			g_num_nodes = atoi(optarg);
			break;
		case 'v':
			g_verbose = 1;
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

	/* seed random number generator */
	srandom(time(NULL));

	worker_init();
	printf("testing single-proposer case...\n");
	ret = run_paxos(0);
	if (ret) {
		fprintf(stderr, "run_paxos(0) failed with error code %d\n",
			ret);
		return EXIT_FAILURE;
	}
	printf("testing multi-proposer case...\n");
	ret = run_paxos(1);
	if (ret) {
		fprintf(stderr, "run_paxos(1) failed with error code %d\n",
			ret);
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
