Project for building a framework for real time messaging and machine learning using Spark Streaming. Currently supports the following:

1. Training dataset read and assessment 
2. Building a Logisitic Regression model for a Binary Classifier to predict whether users will monestise or not
3. Open a ServiceBus Stream Receiver to receive messages on the service bus
4. Enumerate the underlying RDDs and read each individual message running the following query

select Message, LevelTimeRemaining, LevelScore, LevelCompleteness, GameId, GamerId from gameevents where Message='Monetize' or Message='MonetizeDeclined'

5. Send a service bus message which will send GamerId, GameId and 0/1 ShouldMonetize attribute in JSON 
