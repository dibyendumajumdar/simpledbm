/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.tools.diagnostics;

import java.text.MessageFormat;

import org.simpledbm.rss.util.WrappingSequencer;
import org.simpledbm.rss.util.logging.Logger;

/**
 * An efficient thread safe but lock free mechanism to generate trace messages.
 * Uses a ring buffer.  Messages are stored in memory so that there is very
 * little performance impact. Each message is tagged with the thread id, and a
 * sequence number. The sequence number wraps around when it reaches
 * 2147483646.
 * <p>
 * Trace messages can be dumped to the log by invoking dump(). The messages are
 * only output if a logger named org.simpledbm.rss.trace has level set to DEBUG.
 * <p>
 * The design of Trace mechanism was inspired by the article in <cite>DDJ April 23, 2007,
 * Multi-threaded Debugging Techniques by Shameem Akhter and Jason Roberts</cite>. This
 * article is an excerpt from the book <cite>Multi-Core Programming</cite> by the same authors.
 * 
 * @author dibyendu majumdar
 * @since 26 July 2008
 */
public class Trace {

	static final Logger log = Logger.getLogger("org.simpledbm.rss.trace");

	/**
	 * A atomic sequence number. 
	 */
	static final WrappingSequencer seq = new WrappingSequencer(
			Integer.MAX_VALUE);

	/**
	 * Default size of the trace buffer.
	 */
	static final int SIZE = 5000;

	/**
	 * The trace buffer array where events are stored.
	 */
	static TraceElement[] traceBuffer;

	/*
	 * For performance reasons, we only allow numeric arguments to be supplied
	 * as trace message identifiers and arguments. This avoids expensive object allocations.
	 * The trace message is inserted at the next position in the traceBuffer
	 * array, the next pointer wraps to the beginning of the array when it reaches 
	 * the end.
	 */
	
	public static void dump() {
		int max = seq.get();
		for (int i = 0; i < traceBuffer.length; i++) {
			TraceElement e = traceBuffer[i];
			if (e.seq < max && e.msg != -1) {
				String msg = messages[e.msg];
				log.debug(Trace.class.getName(), "dump", MessageFormat.format(msg,
					e.tid, e.seq, e.d1, e.d2, e.d3, e.d4));
			}
		}
	}

	public static void event(int msg) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset].init(next, msg);
	}

	public static void event(int msg, int d1) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset].init(next, msg, d1);
	}

	public static void event(int msg, int d1, int d2) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset].init(next, msg, d1, d2);
	}

	public static void event(int msg, int d1, int d2, int d3) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset].init(next, msg, d1, d2, d3);
	}

	public static void event(int msg, int d1, int d2, int d3, int d4) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset].init(next, msg, d1, d2, d3, d4);
	}

	static final class TraceElement {
		volatile long tid;
		volatile int seq;
		volatile int msg = -1;
		volatile int d1;
		volatile int d2;
		volatile int d3;
		volatile int d4;

		void init(int seq, int msg) {
			this.tid = Thread.currentThread().getId();
			this.seq = seq;
			this.msg = msg;
			this.d1 = this.d2 = this.d3 = this.d4 = 0;
		}

		void init(int seq, int msg, int d1) {
			this.tid = Thread.currentThread().getId();
			this.seq = seq;
			this.msg = msg;
			this.d1 = d1;
			this.d2 = this.d3 = this.d4 = 0;
		}

		void init(int seq, int msg, int d1, int d2) {
			this.tid = Thread.currentThread().getId();
			this.seq = seq;
			this.msg = msg;
			this.d1 = d1;
			this.d2 = d2;
			this.d3 = this.d4 = 0;
		}

		void init(int seq, int msg, int d1, int d2, int d3) {
			this.tid = Thread.currentThread().getId();
			this.seq = seq;
			this.msg = msg;
			this.d1 = d1;
			this.d2 = d2;
			this.d3 = d3;
			this.d4 = 0;
		}

		void init(int seq, int msg, int d1, int d2, int d3, int d4) {
			this.tid = Thread.currentThread().getId();
			this.seq = seq;
			this.msg = msg;
			this.d1 = d1;
			this.d2 = d2;
			this.d3 = d3;
			this.d4 = d4;
		}
	}

	static String[] messages = new String[123];

	static {
		traceBuffer = new TraceElement[SIZE];
		for (int i = 0; i < SIZE; i++) {
			traceBuffer[i] = new TraceElement();
		}

		/*
		 * Following is a temporary solution. Ideally the messages should be stored in
		 * an external file.
		 * Each trace message is tagged with its id, so that we can relate them
		 * back to the code where they are invoked.
		 */
		messages[0] = "0   TID {0} SEQ {1} redo split: splitting page ({2},{3})";
		messages[1] = "1   TID {0} SEQ {1} redo split: initializing new sibling page ({2},{3})";
		messages[2] = "2   TID {0} SEQ {1} redo merge: updating space map page ({2},{3})";
		messages[3] = "3   TID {0} SEQ {1} redo merge: merging into page ({2},{3})";
		messages[4] = "4   TID {0} SEQ {1} redo merge: deallocating merged page ({2},{3})";
		messages[5] = "5   TID {0} SEQ {1} redo link: adding link R ({2}) to parent page ({3},{4})";
		messages[6] = "6   TID {0} SEQ {1} redo unlink: removing link R ({2}) to parent page ({3},{4})";
		messages[7] = "7   TID {0} SEQ {1} redo distribute: inserting key into left sibling page ({2},{3})";
		messages[8] = "8   TID {0} SEQ {1} redo distribute: removing key from left sibling page ({2},{3})";
		messages[9] = "9   TID {0} SEQ {1} redo distribute: removing key from right sibling page ({2},{3})";
		messages[10] = "10  TID {0} SEQ {1} redo distribute: inserting key into right sibling page ({2},{3})";
		messages[11] = "11  TID {0} SEQ {1} redo increase tree height: initializing root page ({2},{3})";
		messages[12] = "12  TID {0} SEQ {1} redo increase tree height: initializing new child (left sibling) ({2},{3})";
		messages[13] = "13  TID {0} SEQ {1} redo decrease tree height: updating root page ({2},{3}) with contents of child page";
		messages[14] = "14  TID {0} SEQ {1} redo decrease tree height: marking child page ({2},{3}) as deallocated";
		messages[15] = "15  TID {0} SEQ {1} redo insert: inserting key into page ({2},{3})";
		messages[16] = "16  TID {0} SEQ {1} redo undo insert: removing inserted key from page ({2},{3})";
		messages[17] = "17  TID {0} SEQ {1} undo insert: fixing exclusively as P ({2},{3})";
		messages[18] = "18  TID {0} SEQ {1} undo insert: searching page ({2},{3})";
		messages[19] = "19  TID {0} SEQ {1} undo insert: original page not available or key has moved, need to search the tree";
		messages[20] = "20  TID {0} SEQ {1} undo insert: upgrading latch on P ({2},{3}) from update to exclusive";
		messages[21] = "21  TID {0} SEQ {1} redo delete: removing key from page ({2},{3})";
		messages[22] = "22  TID {0} SEQ {1} redo undo delete: re-inserting key in page ({2},{3})";
		messages[23] = "23  TID {0} SEQ {1} undo delete: fixing exclusively as P ({2},{3})";
		messages[24] = "24  TID {0} SEQ {1} undo delete: key cannot be removed from original page";
		messages[25] = "25  TID {0} SEQ {1} undo delete: upgrading latch on P ({2},{3}) from update to exclusive";
		messages[26] = "26  TID {0} SEQ {1} do split: extending container";
		messages[27] = "27  TID {0} SEQ {1} do split: upgrading latch on Q ({2},{3}) from update to exclusive";
		messages[28] = "28  TID {0} SEQ {1} do split: latching new right sibling R ({2},{3}) in exclusive mode";
		messages[29] = "29  TID {0} SEQ {1} do split: downgrading latch on Q ({2},{3})";
		messages[30] = "30  TID {0} SEQ {1} do split: downgrading latch on R ({2},{3})";
		messages[31] = "31  TID {0} SEQ {1} do split: do merge: upgrading update latch on Q ({2},{3}) to exclusive mode";
		messages[32] = "32  TID {0} SEQ {1} do split: do merge: upgrading update latch on R ({2},{3}) to exclusive mode";
		messages[33] = "33  TID {0} SEQ {1} do split: do merge: downgrading exclusive latch on Q ({2},{3})";
		messages[34] = "34  TID {0} SEQ {1} do link: upgrading update latch on P ({2},{3}) to exclusive";
		messages[35] = "35  TID {0} SEQ {1} do link: downgrading exclusive latch on P ({2},{3})";
		messages[36] = "36  TID {0} SEQ {1} do unlink: upgrading update latch on P ({2},{3}) to exclusive";
		messages[37] = "37  TID {0} SEQ {1} do redistribute: upgrading update latch on Q ({2},{3}) to exclusive";
		messages[38] = "38  TID {0} SEQ {1} do redistribute: upgrading update latch on R ({2},{3}) to exclusive";
		messages[39] = "39  TID {0} SEQ {1} do redistribute: downgrading exclusive latch on Q ({2},{3})";
		messages[40] = "40  TID {0} SEQ {1} do redistribute: downgrading exclusive latch on R ({2},{3})";
		messages[41] = "41  TID {0} SEQ {1} do increase tree height: extending container";
		messages[42] = "42  TID {0} SEQ {1} do increase tree height: upgrading update latch on P ({2},{3})";
		messages[43] = "43  TID {0} SEQ {1} do increase tree height: fixing new child page exclusively as Q ({2},{3})";
		messages[44] = "44  TID {0} SEQ {1} do increase tree height: downgrading exclusive latch on Q ({2},{3})";
		messages[45] = "45  TID {0} SEQ {1} do increase tree height: downgrading exclusive latch on R ({2},{3})";
		messages[46] = "46  TID {0} SEQ {1} do decrease tree height: upgrading update latch on P (root) ({2},{3})";
		messages[47] = "47  TID {0} SEQ {1} do decrease tree height: upgrading update latch on Q (child) ({2},{3})";
		messages[48] = "48  TID {0} SEQ {1} do decrease tree height: downgrading exclusive latch on P (root) ({2},{3})";
		messages[49] = "49  TID {0} SEQ {1} do split parent: about to split P ({2},{3})";
		messages[50] = "50  TID {0} SEQ {1} entering do repair page underflow";
		messages[51] = "51  TID {0} SEQ {1} do repair page underflow: Q is not the rightmost child of P";
		messages[52] = "52  TID {0} SEQ {1} do repair page underflow: fixing R ({2},{3}) for update";
		messages[53] = "53  TID {0} SEQ {1} do repair page underflow: fig 13 R is an indirect child of P";
		messages[54] = "54  TID {0} SEQ {1} do repair page underflow: merge Q with R";
		messages[55] = "55  TID {0} SEQ {1} do repair page underflow: R is direct child of P";
		messages[56] = "56  TID {0} SEQ {1} do repair page underflow: fig 14 R has a right sibling S that is an indirect child of P";
		messages[57] = "57  TID {0} SEQ {1} do repair page underflow: split P";
		messages[58] = "58  TID {0} SEQ {1} do repair page underflow: After P was split, R is not a child of P anymore, must restart the algorithm";
		messages[59] = "59  TID {0} SEQ {1} do repair page underflow: link S to P";
		messages[60] = "60  TID {0} SEQ {1} do repair page underflow: unlink R from P";
		messages[61] = "61  TID {0} SEQ {1} do repair page underflow: merge Q and R";
		messages[62] = "62  TID {0} SEQ {1} do repair page underflow: Q is the rightmost child of P";
		messages[63] = "63  TID {0} SEQ {1} do repair page underflow: fixing page L (page left of Q as per P) for update as Q ({2},{3})";
		messages[64] = "64  TID {0} SEQ {1} do repair page underflow: fixing page N (page right of L as per L) for update as R ({2},{3})";
		messages[65] = "65  TID {0} SEQ {1} do repair page underflow: fig 17 L is direct child of P and Q is right sibling of L (N == Q)";
		messages[66] = "66  TID {0} SEQ {1} do repair page underflow: Q is no longer about to underflow";
		messages[67] = "67  TID {0} SEQ {1} do repair page underflow: unlink Q from P";
		messages[68] = "68  TID {0} SEQ {1} do repair page underflow: fig 18 about to underflow rightmost child is unlinked from its parent";
		messages[69] = "69  TID {0} SEQ {1} do repair page underflow: merge L and Q";
		messages[70] = "70  TID {0} SEQ {1} do repair page underflow: fig 19 left sibling L of Q has right sibling N that is indirect child of P, Q is right sibling of N";
		messages[71] = "71  TID {0} SEQ {1} do repair underflow: split parent";
		messages[72] = "72  TID {0} SEQ {1} do repair page underflow: link N to P";
		messages[73] = "73  TID {0} SEQ {1} do repair page underflow: fixing page Q for update as R ({2},{3})";
		messages[74] = "74  TID {0} SEQ {1} do repair page underflow: Q is no longer about to underflow";
		messages[75] = "75  TID {0} SEQ {1} do repair page underflow: unlink Q from P";
		messages[76] = "76  TID {0} SEQ {1} do repair page underflow: merge N and Q";
		messages[77] = "77  TID {0} SEQ {1} repair page underflow: restarting algorithm";
		messages[78] = "78  TID {0} SEQ {1} update mode traverse: fixing root page in update mode as P ({2},{3})";
		messages[79] = "79  TID {0} SEQ {1} update mode traverse: root page has right sibling - fixing page ({2},{3}) in update mode as R";
		messages[80] = "80  TID {0} SEQ {1} update mode traverse: fixing child page ({2},{3}) in update mode as Q";
		messages[81] = "81  TID {0} SEQ {1} update mode traverse: Q is only child of root page P, tree height needs to be decreased";
		messages[82] = "82  TID {0} SEQ {1} update mode traverse: fixing child page ({2},{3}) in update mode as Q";
		messages[83] = "83  TID {0} SEQ {1} update mode traverse: Q ({2},{3}) is about to underflow";
		messages[84] = "84  TID {0} SEQ {1} update mode traverse: Q has a right sibling R that is an indirect child of P";
		messages[85] = "85  TID {0} SEQ {1} update mode traverse: P cannot accomodate R's key, so P must be split";
		messages[86] = "86  TID {0} SEQ {1} update mode traverse: link R to P";
		messages[87] = "87  TID {0} SEQ {1} update mode traverse: Q covers search key";
		messages[88] = "88  TID {0} SEQ {1} update mode traverse: Q doesn't cover search key anymore, so fixing R ({2},{3}) in update mode";
		messages[89] = "89  TID {0} SEQ {1} read mode traverse: fixing root page ({2},{3}) in shared mode";
		messages[90] = "90  TID {0} SEQ {1} read mode traverse: moving to the right as search key is greater than high key, fixing page ({2},{3}) in shared mode";
		messages[91] = "91  TID {0} SEQ {1} read mode traverse: fixing child page ({2},{3}) in shared mode";
		messages[92] = "92  TID {0} SEQ {1} insert mode traverse: splitting page ({2},{3}) as it cannot accomodate search key";
		messages[93] = "93  TID {0} SEQ {1} insert mode traverse: upgrading latch to exclusive mode on page ({2},{3})";
		messages[94] = "94  TID {0} SEQ {1} do next key lock: as next key is in right sibling, fixing right sibling page ({2},{3}) in shared mode";
		messages[95] = "95  TID {0} SEQ {1} do next key lock: acquiring lock on next key in no wait mode, location: ({2}.{3}.{4})";
		messages[96] = "96  TID {0} SEQ {1} do next key lock: attempting to acquire lock on next key unconditionally as conditional attempt failed, location: ({2}.{3}.{4})";
		messages[97] = "97  TID {0} SEQ {1} do next key lock: do next key lock: reacquiring exclusive mode latch on page ({2},{3})";
		messages[98] = "98  TID {0} SEQ {1} do next key lock: reacquiring shared mode latch on next page ({2},{3})";
		messages[99] = "99  TID {0} SEQ {1} do next key lock: okay to continue after unconditional lock wait";
		messages[100] = "100 TID {0} SEQ {1} do next key lock: pages have changed while acquiring next key lock, hence must restart insert/delete";
		messages[101] = "101 TID {0} SEQ {1} do next key lock: releasing lock on next key, location: ({2},{3},{4})";
		messages[102] = "102 TID {0} SEQ {1} insert: next key is in next page";
		messages[103] = "103 TID {0} SEQ {1} insert: next key is INFINITY";
		messages[104] = "104 TID {0} SEQ {1} insert: possible duplicate key - lock key in shared more conditionally, location: ({2}.{3}.{4})";
		messages[105] = "105 TID {0} SEQ {1} insert: failed to acquire conditional lock, try acquiring unconditionally, location: ({2}.{3}.{4})";
		messages[106] = "106 TID {0} SEQ {1} insert: walk down the tree again to find the page where insert is to take place";
		messages[107] = "107 TID {0} SEQ {1} insert: fix root page ({2},{3}) in update mode";
		messages[108] = "108 TID {0} SEQ {1} insert: unique constraint violation";
		messages[109] = "109 TID {0} SEQ {1} insert: releasing lock as cursor mode is read committed, location: ({2}.{3}.{4})";
		messages[110] = "110 TID {0} SEQ {1} insert: insert: key no longer exists, rollback and restart";
		messages[111] = "111 TID {0} SEQ {1} insert: releasing lock on next key, location: ({2}.{3}.{4})";
		messages[112] = "112 TID {0} SEQ {1} insert: inserting key";
		messages[113] = "113 TID {0} SEQ {1} delete: deleting key";
		messages[114] = "114 TID {0} SEQ {1} bcursor: removing P ({2},{3})";
		messages[115] = "115 TID {0} SEQ {1} bcursor: set P ({2},{3})";
		messages[116] = "116 TID {0} SEQ {1} bcursor: removing Q ({2},{3})";
		messages[117] = "117 TID {0} SEQ {1} bcursor: set Q ({2},{3})";
		messages[118] = "118 TID {0} SEQ {1} bcursor: removing R ({2},{3})";
		messages[119] = "119 TID {0} SEQ {1} bcursor: set R ({2},{3})";
		messages[120] = "120 TID {0} SEQ {1} bcursor: unfix P ({2},{3})";
		messages[121] = "121 TID {0} SEQ {1} bcursor: unfix Q ({2},{3})";
		messages[122] = "122 TID {0} SEQ {1} bcursor: unfix R ({2},{3})";
	};

}