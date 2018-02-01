# Recommendation-System-based-on-Amazon-Reviews

This recommendation system suggests new items to the users based on their previous purchases and reviews. 
We try to identify similar users by comparing the previous purchases and reviews. Based on the purchases of similar users, we recommend products.

This approach is called Item-based Collaborative filtering approach.

Sometimes the users give a bad rating but write a good review (this may be because the average rating the user gives for any product is usually low) or vice-versa. 
To avoid such cases, we tried to normalize the users ratings by considering the reviews too. To identify the sentiment of the user review, we used the Stanford NLP package.

The Stanford NLP package takes a sentence, identifies the sentiment and returns a value in the range of [-2,2] where -2 refers to very negative and +2 refers to very positive.
Since the user reviews were a combination of sentences and there are hundreds of thousands of user reviews, it would take the system infinitely long to generate the sentiments for all reviews.
To reduce this time, we wrote an algorithm which splits the reviews to multiple smaller sentences and then takes the array of sentiment scores, processes it and returns one final sentiment value.

Using this sentiment score and the average rating given by a particular user for different products, we try to normalize the ratings.

After the normalized ratings are obtained, we try to identify the similar users using the collaborative filtering approach.


To implement this system, we used Java as the programming language and the Hadoop Map-Reduce model.

Dataset : Amazon Reviews Data (Size ~16GB)
          http://jmcauley.ucsd.edu/data/amazon/
          
Contributors : Nikhila Chireddy, Avinash Konduru
          

