
# Overview

This is the web application template for the Programming Track for HW
assignments 3 and 4 for [W4111 - Introduction to Databases, Section 002,
Spring 2021.](https://donald-f-ferguson.github.io/W4111S21/)

The task is to complete the implementation of the specific in the 
[HW definition.](https://courseworks2.columbia.edu/courses/122916/assignments/621219) The
implementation requires implementing four sets of [REST](https://www.restapitutorial.com/)
Resources (Services):
1. _FantasyService_ contains resources implementing a simple
[fantasy baseball](https://en.wikipedia.org/wiki/Fantasy_baseball) league.
2. _LahmanService_ contains resources for querying information from the Lahman's 
Baseball Database.
3. _LikesFollows_ contains resources for implementing a simple network of people who "like"
or "follow" other fantasy baseball participants, as well as fantasy and real world
teams and players.

The HW definition specifies the exact resource functions and paths.

# Packages, Directories and Files

## Not Relevant

You can ignore the following directories:
- middleware
- static

# app.py

This is the main web application and uses [Flask.](https://flask.palletsprojects.com/en/1.1.x/)
There are example [route](https://flask.palletsprojects.com/en/1.1.x/quickstart/#routing)
handlers that help understand and get started with completing the homework. The comments explain the
routes and their implementations.

# Services

## DataServices

This package contains helper libraries for read, querying and writing to various databases, including:
- RDBDataTable.py provides functions for accessing MySQL.
- MongoDBTable (in progress) provides functions for accessing MongoDB.
- Neo4jDataTable (in progress) provides functions for Neo4j.

HW3 only requires RDBDataTable and Neo4jDataTable. Comments in the files explain the functions.

BaseDataTable.py is an abstract class that defines a logical interface.


## LahmanService

This package contains classes that provide access to individual tables in Lahman's Baseball Database by
using RDBDataTable.

## FantasyService

This package contains classes that provide access to individual tables in the fantasy baseball DB by
using RDBDataTable.

## CommentLikeService

This package contains classes that implement the commenting, liking, following, etc. functions.

# unit_tests

This package contains files that demonstrate how to unit test various services and classes.





