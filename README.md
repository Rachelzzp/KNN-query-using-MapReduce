# KNN-query-using-MapReduce
Implement KNN query by using MapReduce that runs on Hadoop

In the KNN query, the input is a set of points (𝑃𝑃) in the Euclidean space, a query point (𝑞𝑞), and an integer (𝑘𝑘). The output is the 𝑘𝑘 points in 𝑃𝑃 that are closest to the query point 𝑞𝑞. A naïve solution is to compute the distance from each point 𝑝𝑝 ∈ 𝑃𝑃 to the query point 𝑞𝑞, sort all the points by their distance, and choose the top 𝑘𝑘 points. However, this naïve solution might not be directly applicable in MapReduce.

Map function:

Use the map function to calculate the distance between two points.
The input is the current id and the number of the current id and the output is the <key, value>. Data will be sent from map to combiner. Data will be sorted the first time and then the combiner will send restriction of the number of the data to each id, which decrease the working load in the reduce function and make the sorting more efficiently.

Reduce function:

Use the result of the map as the input, and output is the <key, value> which value includes the distance and model I defined. Then use the treemap to sort the distance of the points, select and then return the k points.

Totally 4 mappers and 1 reducer are needed.
