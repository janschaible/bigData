import sentiment.Analyzer

object SentimentAnalysis {
  def main(args: Array[String]) = {
    val analyzer = new Analyzer()
    analyzer.analyze()
    analyzer.stop()
  }
}
