package sentiment

/*
target: the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)
ids: The id of the tweet ( 2087)
date: the date of the tweet (Sat May 16 23:58:44 UTC 2009)
flag: The query (lyx). If there is no query, then this value is NO_QUERY.
user: the user that tweeted (robotickilldozr)
text: the text of the tweet (Lyx is cool)
 */

case class Tweet (
  val target: Int,
  val id: String,
  val date: String,
  val flag: String,
  val user: String,
  val text: String
)
