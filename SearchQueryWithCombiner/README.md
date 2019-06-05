#[Task](https://docs.google.com/document/d/1vCshBZMrH-30y8oAHng5oXfukLmU1mVlqJaxi3bw9z8/edit)

#Schema 
#####Mapper: splits query to the pairs userId-word
#####Combiner: counts each word and generates text like: `word\t<count>`
#####Reducer: merges all the same words together and sum the counts accordingly and choose top 3 words

#Build jar:
`mvn clean compile assembly:single`