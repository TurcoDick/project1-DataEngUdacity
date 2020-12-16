# Project - Organizing data in Sparkify
## Introduction.
The objective of the project is to develop a data architecture that organizes and simplifies the consultations for the songs heard.

This project is for a fictional company called Sparkify, it provides its customers with the convenience of listening to their music through the app on their smartphone.

## objective

The goal is to organize Sparkify data in a way that makes it easy for analysts to produce reports with metrics of songs heard by customers. Today the Sparkify data architecture is divided into two places, the first comes from the app and contains, among other things, user data and the second is the Sparkify database that has the music data. A simple query takes a lot of time to analyze and study the analyst in the data.
With the new architecture, everything is easier, because the data is clearly separated in the dimension tables, and depending on the query he wants he can use a simple query in the fact table.


### Learning.
Now the role of the data engineer has become very clear, they must prepare the data so that whoever uses it will focus on their goals.

### The project consists of the following files:
**create_tables.py**  = responsible for deleting, creating and recreating the database and tables.

**etl.py** = responsible for the etl process in the data files, it is in this file that the following processes take place:
Obtaining the data;
Data cleaning;
Transformation of data required for a column in the new table;
Call the dependencies to make the CRUD in the database;

**sql_queries.py** = responsible for saving CRUD queries to the PostgreSQL database;

**test.ipynb** = responsible for testing whether the database and its tables were created and whether the data is in it.

### Running the project
Open the terminal and execute the commands:
1. python3 create_tables.py
2. python3 etl.py


### Table architecture

#### The dimension tables are as follows:
* **users** </br>
* **songs** </br>
* **artists** </br>
* **time** </br>
* **Log** </br>

#### The actual table is:
* **songplays**

![](./table%20architecture.png)


