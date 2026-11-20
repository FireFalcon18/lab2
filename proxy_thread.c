/*****************************************************************************\
 * Redes Integradas de Telecomunicacoes
 * MEEC/MERSIM - FCT NOVA  2026/2027
 *
 * proxy_thread.c
 *
 * Functions that implement the proxy threads, which bind IPv4 clients to IPv6 servers
 *
 * @author  Luis Bernardo
\*****************************************************************************/

#include <pthread.h>
#include <gtk/gtk.h>
#include <arpa/inet.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include "sock.h"
#include "gui.h"
#include "callbacks.h"
#include "callbacks_socket.h"
#include "proxy_thread.h"


GList *plist= NULL;			// List of active proxy threads
pthread_mutex_t mutex5= PTHREAD_MUTEX_INITIALIZER;	// For Proxy list

/******************************************\
|* Functions that handle the thread list  *|
\******************************************/

// Create a new thread state object
thread_state *new_thread_state(int sock4, struct sockaddr_in6 *cli_addr) {
	assert(sock4 >= 0);
	thread_state *pt = (thread_state *) malloc(sizeof(thread_state));
	memcpy(&pt->cli_ip, &cli_addr->sin6_addr, 16);
	pt->cli_port= ntohs(cli_addr->sin6_port);
	pt->sock4 = sock4;
	pt->sock6 = -1;
	pt->filename = NULL;
	pt->seq= -1;
	pt->q= NULL;
	pt->status = TH_INIT;

	pt->self = pt;
	pthread_mutex_lock(&mutex5);
	plist= g_list_append(plist, pt);
	pthread_mutex_unlock(&mutex5);
	return pt;
}

// Update the information about the IPv6 server
void update_thread_state(thread_state *pt, int sock6, const char *fname, u_int32_t seq) {
	assert((pt != NULL) && (pt->self == pt));
	pt->sock6 =sock6;
	pt->filename = g_strdup(fname);
	pt->seq= seq;
}

// Search for thread_state descriptor in plist using the filename and sequence number
thread_state *locate_state_in_plist(const char *filename, u_int32_t seq) {
	assert(filename != NULL);
	GList *list;
	pthread_mutex_lock(&mutex5);
	for (list = plist; list != NULL; list = g_list_next(list)) {
		if (((thread_state *) list->data)->seq == seq)
			if (!strcmp(filename, ((thread_state *) list->data)->filename)) {
				pthread_mutex_unlock(&mutex5);
				return (thread_state *) list->data;
			}
	}
	pthread_mutex_unlock(&mutex5);
	return NULL;
}

// Close socket IPv4, optionally sending a 0 length
static void close_sock4(thread_state *pt, gboolean send_len0) {
	if (pt->sock4 >= 0) {
		if (send_len0) {
			long long len0 = 0;
			if (write(pt->sock4, &len0, sizeof(len0)) != sizeof(len0)) {
				printf("close_sock4 failed to write length 0 on sock4\n");
			}
		}
		close(pt->sock4);
		pt->sock4 = -1;
	}
}

// Close socket IPv6
static void close_sock6(thread_state *pt) {
	if (pt->sock6 >= 0) {
		close(pt->sock6);
		pt->sock6 = -1;
	}
}


// Free a thread state object, removing it from the list, clearing all info from the GUI
// and freeing all memory previously allocated
void free_thread_state(thread_state *pt, gboolean called_from_GUI) {
	if ((pt==NULL) || (pt->self != pt))
		return;

	pt->self = NULL;	// It is being freed

	// Remove from proxy thread list
	pthread_mutex_lock(&mutex5);
	plist = g_list_remove(plist, pt);
	pthread_mutex_unlock(&mutex5);

	// Remove from the GUI table
	GUI_del_Proxy(pt->filename, pt->seq, pt->sock4, called_from_GUI);

	// Get pointer to Query
	Query *q= pt->q;
	if ((q == NULL) && (pt->filename != NULL))
		q= locate_qid_in_Querylist_by_IPv(pt->filename, pt->seq, FALSE);
	if (q != NULL) {
		q->thread= NULL;	// to avoid double free of structures !!!
		free_query_in_Querylist(q, called_from_GUI);
	}
	pt->q= NULL;

	close_sock4(pt, FALSE);
	close_sock6(pt);

	// Free memory
	if (pt->filename != NULL) {
		free(pt->filename);
		pt->filename= NULL;
	}
	free(pt);
}


// Stop a thread identified by the thread_state
void stop_thread_by_id(thread_state *pt) {
	if ((pt == NULL) || (pt->self != pt))
		return;

	if (pt->sock4 > 0) {
		// Wake a thread blocked in poll(), select(), or read(), if needed.
		if (shutdown(pt->sock4, SHUT_RDWR)) {
			// shutdown failed to stop flows
			close_sock4(pt, FALSE);
		}
	}
	if (pt->sock6 > 0) {
		// Wake a thread blocked in poll(), select(), or read(), if needed.
		if (shutdown(pt->sock6, SHUT_RDWR)) {
			// shutdown failed to stop flows
			close_sock6(pt);
		}
	}

	// To guarantee that the memory is freed only when it is no longer needed
	// free_thread_state is called inside the thread function
}


// Stop a thread identified by the filename and the sequence number
gboolean stop_thread(const char *filename, u_int32_t seq) {
	thread_state *pt= locate_state_in_plist(filename, seq);
	if (pt == NULL)
		return FALSE;

	stop_thread_by_id(pt);
	return TRUE;
}


// Close all threads
void close_all_threads(gboolean called_from_GUI) {
	while (plist != NULL) {
		thread_state *pt= (thread_state *)plist->data;
		if (pt == NULL)
			continue;
		stop_thread_by_id(pt);
	}
}


/*****************************************************************************************\
|* Functions that implement the proxy and handle the communication between IPv4 and IPv6 *|
\*****************************************************************************************/


// Create a connection and return the socket
int connect_to_ipv6_server(const char *ip, uint port) {
	// Creates TCP socket
	struct hostent *hp, *gethostbyname2();
	struct sockaddr_in6 server;
	int sockTCP;

	assert(ip != NULL);

	/* Connect socket using the name specified in the command line. */
	hp = gethostbyname2(ip, AF_INET6);
	if (hp == 0) {
		fprintf(stderr, "%s: unknown host\n", ip);
		return -1;
	}
	server.sin6_family = AF_INET6;
	server.sin6_flowinfo = 0;
	server.sin6_port = htons(port);
	bcopy(hp->h_addr, &server.sin6_addr, hp->h_length);
	// Creates TCP socket
	sockTCP = init_socket_ipv6(SOCK_STREAM, 0, FALSE);
	if (sockTCP < 0) {
		Log("Failed opening IPv6 TCP socket\n");
		return -1;
	}

	if (connect(sockTCP, (struct sockaddr *) &server, sizeof(server)) < 0) {
		perror("connecting stream socket");
		fprintf(stderr, "Failed connecting IPv6 TCP socket to %s-%hu\n",
				ip, port);
		close(sockTCP);
		return -1;
	}
	return sockTCP;
}

// Connect to one file server, cycling through all hits received
int connect_to_file_server(thread_state *state, const char *filename, u_int32_t seq) {
	// Locate IPv6 server with file requested
	const char *hits;
	char hits_buf[512];

	if (!GUI_get_Query_hits(filename, seq, FALSE/*IPv4*/, &hits)) {
		// No hits available
		return -1;
	}

	fprintf(stderr, "Filename='%s' Seq=%d Hits=%s\n", filename, seq, hits);
	strncpy(hits_buf, hits, sizeof(hits_buf)-1);

	char *next = hits_buf, *pt, *ip;
	int port;
	int sock = -1;
	do {
		ip = next;
		pt = strchr(ip, '-');
		if (!pt) {
			// Invalid hits format
			return -1;
		}
		*pt = '\0';
		port = strtol(pt + 1, &next, 10);
		while ((next != NULL) && (*next==' '))
			next++;	// Skip spaces

		printf("Trying connection to %s:%d\n", ip, port);
		sock = connect_to_ipv6_server(ip, port);

	} while ((sock < 0) && (next != NULL));

	if (sock >= 0) {
		// Update Proxy information
		GUI_update_serv_details_Proxy(state->sock4, ip, port);
	}
	return sock;
}

// Update the % transmitted on the GUI
gboolean update_transf(thread_state *pt, int transf) {
	if (!GUI_update_transf_Proxy(pt->sock4, transf))
		printf("GUI update transfer failed\n");
	return TRUE;
}

// Configure socket timeouts and buffer sizes (TASK 3.4)
static void configure_tcp_socket(int sock) {
	if (sock < 0)
		return;

	struct timeval tv;
	tv.tv_sec = 10;
	tv.tv_usec = 0;
	if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
		perror("setsockopt SO_RCVTIMEO");
	}

	int bufsize = 262144; // 256 KB
	setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
	setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
}

// Function that implements the thread function:
//		it implements all communications between client IPv4 and server IPv6
//		ptr - pointer to the thread state object
void *proxy_function(void *ptr) {
	assert(ptr != NULL);
	thread_state *pt= (thread_state *)ptr;
	gboolean slow = get_checkbutton_Slow_state();	// get the slow state from the checkbox

	char conn_str[20];		// Temporary buffer with the thread name
	char write_buf[256];	// Write temporary buffer for logging

	struct timeval 	tv1, tv2; // To measure file transfer delay
	struct timezone tz;		  // Auxiliary variable

	uint32_t seq;			// Request header variable - sequence number
	int16_t namelen;		// Request header variable - namelength
	long diff= 0;	// % of bytes received, and the previous one
	Query *q= NULL;

	if (!active || (pt->self != pt)) {
		sprintf(write_buf, "%sInvalid state pointer\n", conn_str);
		Log(write_buf);
		pthread_exit(NULL);
	}

	// Set the string with the connection name
	sprintf(conn_str, "th(%d): ", pt->sock4);

	// Set timeout for reading from socket IPv4
	configure_tcp_socket(pt->sock4);

	//////// Read the request from the filexchange on IPv4 ///////
	// Read seq
	if (!active ||(read(pt->sock4, &seq, sizeof(seq)) != sizeof(seq))) {
		sprintf(write_buf, "%sDid not receive seq\n", conn_str);
		Log(write_buf);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}
	// Read the filename length
	if (!active || (read(pt->sock4, &namelen, sizeof(namelen)) != sizeof(namelen))) {
		sprintf(write_buf, "%sDid not received the filename's length\n",
				conn_str);
		Log(write_buf);
		close_sock4(pt, FALSE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}
	// validate the filename length
	if ((namelen <= 0) || (namelen > 256)) {
		sprintf(write_buf, "%sInvalid filename's length (%d)\n", conn_str, namelen);
		Log(write_buf);
		close_sock4(pt, FALSE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}

	// ** TASK 3.3 **

	// Read the filename
	char filename[257];
	int rlen = 0;
	while (rlen < namelen) {
		int r = read(pt->sock4, filename + rlen, namelen - rlen);
		if (r <= 0) {
			sprintf(write_buf, "%sFailed reading filename\n", conn_str);
			Log(write_buf);
			free_thread_state(pt, FALSE);
			pthread_exit(NULL);
		}
		rlen += r;
	}
	filename[namelen] = '\0';
	// (do not forget to use a buffer to receive the filename)

	// Update Proxy client information in the GUI
	GUI_update_cli_details_Proxy(filename, seq, pt->sock4, addr_ipv6(&pt->cli_ip), pt->cli_port);

	// Locate the Query state associated with the connection
	q = locate_qid_in_Querylist_by_IPv(filename, seq, FALSE);
	if (q == NULL) {
		sprintf(write_buf, "%sQuery not found for '%s'(%u)\n", conn_str, filename, seq);
		Log(write_buf);
		close_sock4(pt, TRUE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}
	stop_query_timer(q);
	q->thread = pt;
	pt->q = q;

	// Connect to fileexchange on IPv6, creating socket pt->sock6.
	pt->sock6 = connect_to_file_server(pt, filename, seq);
	if (pt->sock6 < 0) {
		sprintf(write_buf, "%sFailed connecting to IPv6 server for '%s'\n", conn_str, filename);
		Log(write_buf);
		pt->status = TH_ERROR;
		close_sock4(pt, TRUE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}
	pt->status = TH_CONNECTED;

	// ## part of TASK 10 ##
	// Configure your socket IPv4 to define a timeout time for reading operations and
	// to set buffers or other any configuration that maximizes throughput
	//	e.g. SO_SNDBUF, SO_RECVBUF, timeout, etc.
	configure_tcp_socket(pt->sock6);


	// Update state in thread state list
	// update_thread_state(pt, pt->sock6, buf, seq);
	update_thread_state(pt, pt->sock6, filename, seq);


	// Send the request to the IPv6 filexchange
	if ((write(pt->sock6, &seq, sizeof(seq)) != sizeof(seq)) ||
		(write(pt->sock6, &namelen, sizeof(namelen)) != sizeof(namelen)) ||
		(write(pt->sock6, filename, namelen) != namelen)) {
		sprintf(write_buf, "%sFailed sending request to IPv6 server\n", conn_str);
		Log(write_buf);
		pt->status = TH_ERROR;
		close_sock4(pt, TRUE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}

	// Receive the file length from the IPv6 server
	long long flen = 0;
	if (read(pt->sock6, &flen, sizeof(flen)) != sizeof(flen)) {
		sprintf(write_buf, "%sFailed reading file length from IPv6 server\n", conn_str);
		Log(write_buf);
		pt->status = TH_ERROR;
		close_sock4(pt, TRUE);
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}

	// Send the length to the IPv4 filexchange
	if (write(pt->sock4, &flen, sizeof(flen)) != sizeof(flen)) {
		sprintf(write_buf, "%sFailed sending file length to IPv4 client\n", conn_str);
		Log(write_buf);
		pt->status = TH_ERROR;
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}

	// Test if the file is empty
	if (flen <= 0) {
		sprintf(write_buf, "%sEmpty or unavailable file (len=%lld)\n", conn_str, flen);
		Log(write_buf);
		pt->status = (flen == 0) ? TH_FINISHED : TH_ERROR;
		free_thread_state(pt, FALSE);
		pthread_exit(NULL);
	}


 	// get the starting time
	if (gettimeofday(&tv1, &tz))
		Log("Error getting time\n");


	// Receive the file from the server and forward it to the client
	char data_buf[FILE_BUFLEN];
	long long total_sent = 0;
	int last_perc = -1;
	pt->status = TH_TRANSFERRING;

	do {
		// Loop forever until end of file or socket error
		// read the file from the socket IPv6 to send to the socket IPv4
		// READ and ADAPT the code in Section 2.1.9 "Read and write binary files"
		long long remaining = flen - total_sent;
		size_t to_read = (remaining < FILE_BUFLEN) ? (size_t) remaining : FILE_BUFLEN;
		int n = read(pt->sock6, data_buf, to_read);
		if (n > 0) {
			int written = 0;
			while (written < n) {
				int m = write(pt->sock4, data_buf + written, n - written);
				if (m <= 0) {
					sprintf(write_buf, "%sWrite error to IPv4 client\n", conn_str);
					Log(write_buf);
					pt->status = TH_ERROR;
					break;
				}
				written += m;
			}
			if (written < n)
				break;

			total_sent += n;
			// calculate diff - the percentage of the file received
			diff = (long) ((total_sent * 100) / flen);
			if (diff != last_perc) {
				last_perc = (int) diff;
				// Update the transmission % on the GUI
				update_transf(pt, (int) diff);
			}
		} else {
			sprintf(write_buf, "%sRead error or premature EOF from IPv6 server\n", conn_str);
			Log(write_buf);
			pt->status = TH_ERROR;
			break;
		}

		if (slow) // Make the transmission slow - to facilitate debug
			usleep(SLOW_SLEEPTIME);
	} while (active && (pt->self == pt) && (total_sent < flen));

	if (total_sent == flen) {
		pt->status = TH_FINISHED;
	}

	// Get the ending time
	if (gettimeofday(&tv2, &tz)) {
		g_print("%sError getting time\n", conn_str);
		diff= 0;
	} else
		diff= (tv2.tv_sec-tv1.tv_sec)*1000000+(tv2.tv_usec-tv1.tv_usec);
	g_print("%sproxy ended - lasted %ld usec\n", conn_str, diff);

	if (pt == pt->self) {
		// Wrap up
		free_thread_state(pt, FALSE);
	}

	return NULL;
}