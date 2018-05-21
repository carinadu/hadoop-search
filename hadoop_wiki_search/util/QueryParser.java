package util;
/**
 * 1. new version of QueryParser supports "not" operations in parentheses


 *    a. added new enumerate (WORD, EXCEPT_WORD, STOP_WORD) type of Expr
 *    b. adjusted static class PostingListWrapper that add type property to handle different type of
 *       the posting list. 0 means normal posting list, all the result in the posting list should
 *       occur in the result; 1 means all the documents in the posting list should not occur in the
 *       result; 2 means the posting list should not be considered.
 *    c. check each possible case and evaluate by three underling operations intersect, merge, difference
 * 2. optimized parseQuery method to fix the problem - not(A and B) same as not A and B
 * 3. added stop words handling
 * 4. added method sortPostingByScore in PostingList class to support sort postings.
 * 5. allow phrase query, basic idea is: use the position information of each posting, add more weight if 
 * position of two words is smaller.
 * 6. Fix the parse abnormal bug. Bug: Query with words like "normal" can not be parsed correctly because
 * it contains deliminator "or". Now parse query with "\\bor\\b",  "\\band\\b"
 * 7. Fix the bug that query like "A and B or not C". "not C" cannot be parsed correctly because it was
 * treated as a phrase. Fixed by checking whether or terms are start with not, if yes, query after that not
 * cannot be a phrase (can only be a word).
 */
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import util.PostingList.Posting;

public class QueryParser {
	/*
	 * QueryParser: parse conjunctive normal form into a hierarchy structure
	 * rank is as following:
	 * In this version, we're not using cosine similarity for simplicity.
	 * and(A, B) = score(A) * score(B)
	 * or(A, B) = score(A) + score(B)
	 * not and phrase are the hardest part and wait for design
	 */
	public static final Set<String> STOP_WORD_SET = new HashSet<String>();
	//This property is used for adjust the influence of distance of words to the score of a document
	//Lower: distance will affect more; higher: distance will affect less
	private static double phraseFactor = 1.0;
	/**
	 * AND: A and B
	 * OR: A or B
	 * NOT_OR: not(A or B)
	 * TERM/NOT_TERM: leaf node, TERM->the result documents should contain the term, NOT_TERM->result should not contain the term
	 */
	private enum Expr {
		
		AND, OR, NOT_OR, WORD, EXCEPT_WORD, STOP_WORD;
	}
	
	/**
	 * neg: whether the document in the posting list should be in the result
	 *      true: should not appear
	 *      false: should appear
	 * list: a list of documents id and scores
	 */
	public static class PostingListWrapper {
		int type;//0 : normal; 1 : negative; 2: stop word
		public PostingList list;
		PostingListWrapper(int neg, PostingList list) {
			this.type = neg;
			this.list = list;
		}
	}
	
	public static class Query {
		private Expr expr;
		private String word;
		private Query[] next;
		
		private Query(Expr expr) {
			this.expr = expr;
		}
		
		private Query(String word) {//check weather there is a not before the word 
			if(STOP_WORD_SET.contains(word)) {
				this.expr = Expr.STOP_WORD;
				this.word = null;
			}
			else if(word.startsWith("not ")) {
				this.expr = Expr.EXCEPT_WORD;
				this.word = word.substring(3).trim();
			}
			else {
				this.expr = Expr.WORD;
				this.word = word;
			}
		}
		
		public PostingListWrapper evaluate(Map<String, PostingList> terms) {
			/**
			 * if expr == NOT_WORD / WORD, leaf nodes, create PostingListWrapper object according to the Expr type with type 1 and 0
			 * otherwise, recursively evaluate the expression
			 * if expr == STOP_WORD, return a new PostingListWrapper with type 2
			 */
			if(expr == Expr.WORD) return new PostingListWrapper(0, terms.getOrDefault(word, new PostingList()));
			else if(expr == Expr.EXCEPT_WORD) return new PostingListWrapper(1, terms.getOrDefault(word, new PostingList()));
			else if(expr == Expr.STOP_WORD) return new PostingListWrapper(2, new PostingList());
			int n = next.length;
			if(expr == Expr.OR || expr == Expr.NOT_OR) {
				PostingListWrapper[] pList = new PostingListWrapper[n];
				for(int i = 0; i < next.length; ++i) pList[i] = next[i].evaluate(terms);
				PostingListWrapper ans = doOr(pList, 0, n - 1);
				if(expr == Expr.NOT_OR) ans.type = 1 - ans.type;
				return ans;
			}else if(expr == Expr.AND) {
				PostingListWrapper[] pList = new PostingListWrapper[n];
				for(int i = 0; i < next.length; ++i)
					pList[i] = next[i].evaluate(terms);
				PostingListWrapper wrapper = doAnd(pList, 0, n - 1);
				return wrapper;
			}
			return null;
		}
		
		private PostingListWrapper intersect(PostingListWrapper pList1, PostingListWrapper pList2) {
			//if both of the two words are stop words, return a empty PostingListWrapper with type 2
			if(pList1.type == 2 && pList2.type == 2) return new PostingListWrapper(2, new PostingList());
			else if(pList1.type == 2) {//if the first word is stop word, return the PostingListWrapper of the second word
				return pList2;
			}
			else if(pList2.type == 2) {//if the second word is stop word, return the PostingListWrapper of the first word
				return pList1;
			}
			else if(pList1.type == 1 && pList2.type == 1) //-A and -B -> -(A merge B)
				return new PostingListWrapper(1, merge(pList1.list, pList2.list));
			else if(pList1.type == 0 && pList2.type == 0)//A and B -> A intersect B
				return new PostingListWrapper(0, intersect(pList1.list, pList2.list));
			else if(pList1.type == 1)//-A and B -> B - A
				return new PostingListWrapper(0, difference(pList2.list, pList1.list));
			else// A and -B -> A - B
				return new PostingListWrapper(0, difference(pList1.list, pList2.list));
		}
		
		/**
		 * Handle all the expressions connected with "and"
		 * @param pList a list of wrapped posting list with their "neg" information and PostingList
		 * @param low start index of the pList (for binary evaluate)
		 * @param high end index of the pList
		 * @return
		 */
		private PostingListWrapper doAnd(PostingListWrapper[] pList, int low, int high) {
			if(low > high) return new PostingListWrapper(0, new PostingList());
			if(low == high) {
				PostingList ans = new PostingList();
				PostingList org = pList[low].list;
				for(int i = 0; i < org.size(); ++i) {
					Posting p = org.get(i);
					ans.addPosting(new Posting(p.getDocId(), p.getScore()));
				}
				return new PostingListWrapper(pList[low].type, ans);
			}
			Arrays.sort(pList, new Comparator<PostingListWrapper>(){
				@Override
				public int compare(PostingListWrapper o1, PostingListWrapper o2) {
					return o1.list.size() - o2.list.size();
				}
			});
			PostingListWrapper ans = pList[0];
			for(int i = 1; i < pList.length; ++i){
				ans = intersect(ans, pList[i]);
			}
				
			return ans;
		}
		
		private PostingList difference(PostingList pList1, PostingList pList2) {
			PostingList ans = new PostingList();
			for(int i = 0, j = 0; i < pList1.size();) {
				if(j == pList2.size()) {
					Posting p = pList1.get(i++);
					ans.addPosting(new Posting(p.getDocId(), p.getScore()));
				}else {
					Posting left = pList1.get(i), right = pList2.get(j);
					int cmp = left.getDocId().compareTo(right.getDocId());
					if(cmp < 0) {
						ans.addPosting(new Posting(left.getDocId(), left.getScore()));
						++i;
					}else if(cmp > 0) ++j;
					else {
						++i; ++j;
					}
				}
			}
			return ans;
		}
		
		private PostingList intersect(PostingList pList1, PostingList pList2) {
			PostingList ans = new PostingList();
			for(int i = 0, j = 0; i < pList1.size() && j < pList2.size();) {
				Posting left = pList1.get(i), right = pList2.get(j);
				System.out.println("left " + left + "; right " + right);
				int cmp = left.getDocId().compareTo(right.getDocId());
				if(cmp < 0) ++i;
				else if(cmp > 0) ++j;
				else {
					System.out.println("i " + i + "; j" + j);
					/**
					 * Calculate the minimum distance of two words in a document
					 */
					List<Integer> positions1 = pList1.get(i).getPositions(), positions2 = pList2.get(j).getPositions();
					int minDis = Integer.MAX_VALUE;
					if(positions1 != null && positions2 != null && positions1.size() > 0 && positions2.size() > 0) {
						int p1 = positions1.get(0), p2 = positions2.get(0);for(int p = 1, q = 1; p < positions1.size() && q < positions2.size();) {	
							/**
							 * only calculate possible nearest positions:
							 * in doc1:
							 * word1: 1 3 4 5 10 11
							 * word2: 7 12
							 * 1 3 4 should not be computed, because 5 is the only possible candidate.
							 */
							if(p1 < p2) {
								p++;
								while(p < positions1.size() && p1 < p2) {
									p1 = p1 + positions1.get(p);
									p++;
								}
								if(p < positions1.size()) {
									p--;
									p1 = p1 + positions1.get(p);
								}
								minDis = Math.min(minDis, Math.abs(p1 - p2));
								p++;
							}
							else if(p1 > p2) {
								q++;
								while(q < positions2.size() && p2 < p1) {
									p2 = p2 + positions2.get(q);
									q++;
								}
								if(q < positions2.size()) {
									q--;
									p2 = p2 + positions2.get(q);
								}
								minDis = Math.min(minDis, Math.abs(p1 - p2));
								q++;
							}
							else{
								p++;
								q++;
							}
						}
					}					
					if(minDis != Integer.MAX_VALUE)
						ans.addPosting(new Posting(left.getDocId(), Math.pow(left.getScore() * right.getScore(), phraseFactor + 1.0 / minDis)));
					else 
						ans.addPosting(new Posting(left.getDocId(), left.getScore() * right.getScore()));
					++i; ++j;
				}
			}
			return ans;
		}

		/**
		 * Handle all the expressions connected with "and"
		 * @param pList Handle all the expressions connected with "or"
		 * @param low start index of the pList (for binary evaluate)
		 * @param high end index of the pList
		 * @return
		 */
		private PostingListWrapper doOr(PostingListWrapper[] pList, int low, int high) {
			if(low == high) {
				PostingList ans = new PostingList();
				PostingList org = pList[low].list;
				for(int i = 0; i < org.size(); ++i) {
					Posting p = org.get(i);
					ans.addPosting(new Posting(p.getDocId(), p.getScore()));
				}
				return new PostingListWrapper(pList[low].type, ans);
			}
			int mid = low + (high - low)/2;
			return merge(doOr(pList, low, mid), doOr(pList, mid + 1, high));
		}
		
		private PostingListWrapper merge(PostingListWrapper pList1, PostingListWrapper pList2) {
			//if both of the two words are stop words, return a empty PostingListWrapper with type 2
			if(pList1.type == 2 && pList2.type == 2) return new PostingListWrapper(2, new PostingList());
			else if(pList1.type == 2) {//if the first word is stop word, return the PostingListWrapper of the second word
				return pList2;
			}
			else if(pList2.type == 2) {//if the second word is stop word, return the PostingListWrapper of the first word
				return pList1;
			}
			else if(pList1.type == 1 && pList2.type == 1) //-A or -B -> -(A and B)
				return new PostingListWrapper(1, intersect(pList1.list, pList2.list));
			else if(pList1.type == 0 && pList2.type == 0)//A or B -> A merge B
				return new PostingListWrapper(0, merge(pList1.list, pList2.list));
			else if(pList1.type == 1)//-A or B -> -(A difference B)
				return new PostingListWrapper(1, difference(pList1.list, pList2.list));
			else// A or -B -> -(B difference A)
				return new PostingListWrapper(1, difference(pList2.list, pList1.list));
		}
		
		private PostingList merge(PostingList pList1, PostingList pList2) {
			PostingList ans = new PostingList();
			for(int i = 0, j = 0; i < pList1.size() || j < pList2.size();) {
				if(i == pList1.size()) {
					Posting p = pList2.get(j++);
					ans.addPosting(new Posting(p.getDocId(), p.getScore()));
				}else if(j == pList2.size()) {
					Posting p = pList1.get(i++);
					ans.addPosting(new Posting(p.getDocId(), p.getScore()));
				}else {
					Posting left = pList1.get(i), right = pList2.get(j);
					int cmp = left.getDocId().compareTo(right.getDocId());
					if(cmp < 0) {
						ans.addPosting(new Posting(left.getDocId(), left.getScore()));
						++i;
					}else if(cmp > 0) {
						ans.addPosting(new Posting(right.getDocId(), right.getScore()));
						++j;
					}else {
						ans.addPosting(new Posting(left.getDocId(), left.getScore() + right.getScore()));
						++i; ++j;
					}
				}
			}
			return ans;
		}
	}
	
	private Stemmer stmr;
	
	public QueryParser() {
		this.stmr = new Stemmer();
	}
	
	/**
	 * Parse the input query, build the operation tree
	 */
	public Query parseQuery(String query) {
		String[] and_terms = query.split("\\band\\b");
		Query root = new Query(Expr.AND);
		root.next = new Query[and_terms.length];
		for(int i = 0; i < and_terms.length; ++i) {
			String or_term = and_terms[i].trim();
			//System.out.println(or_term);
			Expr expr = (or_term.startsWith("not ") 
					|| or_term.startsWith("not(")
					|| or_term.startsWith("not (")) 
					? Expr.NOT_OR : Expr.OR;
			if(expr == Expr.NOT_OR) or_term = or_term.substring(3);
			or_term = or_term.replace('(', ' ').replace(')', ' ').trim();
			Query orQuery = new Query(expr);
			String[] or_terms = or_term.split("\\bor\\b");
			orQuery.next = new Query[or_terms.length];
			for(int j = 0; j < or_terms.length; ++j) {
				String phrase = or_terms[j].trim();
				if(phrase.startsWith("not ")){//if or term contains not, there should not be a phrase after not, only word supported 
					orQuery.next[j] = new Query(phrase);
				}else{//if or term does not contains not, support phrase
					Query andQuery = new Query(Expr.AND);
					String[] words = phrase.split(" ");
					andQuery.next = new Query[words.length];
					for(int k = 0; k < words.length; k++) {//for phrase, vanilla cake
						String word = words[k];//to lower case?
						stmr.add(word.toCharArray(), word.length());
						stmr.stem();
						andQuery.next[k] = new Query(stmr.toString());
					}
					orQuery.next[j] = andQuery;
				}
			}
			root.next[i] = orQuery;
		}
		return root;
	}
	
	public static void main(String args[]) {
		QueryParser parser = new QueryParser();
		Query query = parser.parseQuery("(vanilla or not chocolate cake) and (strawberry)");
		Map<String, PostingList> terms = new HashMap<String, PostingList>();
		terms.put("vanilla", new PostingList("1:2.0:1,5|2:2.0:4,10"));
		terms.put("chocolate", new PostingList("1:2.0:2,6|2:2.0:6,12|4:4.0:1,2"));
		terms.put("strawberri", new PostingList("1:1.4:3,4|2:1.4:8,14|4:2.0:1,2"));
		terms.put("cake", new PostingList("1:1.4:3,4|2:1.4:8,14|4:2.0:1,2"));
		PostingListWrapper list = query.evaluate(terms);
		list.list.sortPostingByScore();
		System.out.println("neg? : " + list.type);
		for(Posting p : list.list.getPosting())
			System.out.println(p.getDocId() + " " + p.getScore());
	}
}
