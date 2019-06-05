#[Task](https://docs.google.com/document/d/1vCshBZMrH-30y8oAHng5oXfukLmU1mVlqJaxi3bw9z8/edit)

#Schema 
#####Mapper: splits query to the pairs userId-word
#####Reducer: reducer calculates top 3 popular words from the list of all word for user

#Build jar:
`mvn clean compile assembly:single`  