# Report

* Author : Bastien Wermeille
* Scyper : 308542

# Specificities

Here is some explanations of the algorithms implemented during this small project.

## Join

For the join, my first implementation was using 2 LazyList building over the 2 inputs (left and right) allowing in case where we only want to retrieve a single entry to be very efficient. This method was however not as efficient when we need to retrieve all entries so my second implementation loads all the entry from the right and then map those into a map entry to prevent a n*m operations.

## Aggregate

For the aggregate, we first load all the entries, build the aggregations of the data. The last step is finally to reduce the data before display.

## Sort

An `Ordering` object is created from the parameters and then all data are retrieve and sorted using this ordering.
