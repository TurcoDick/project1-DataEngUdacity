### Introduction.
The objective of the project is to develop a data architecture that organizes and simplifies the consultations for the songs heard.

This project is for a fictional company called Sparkify, it provides its customers with the convenience of listening to their music through the app on their smartphone.

## objective

The goal is to organize Sparkify data in a way that makes it easy for analysts to produce reports with metrics of songs heard by customers. Today the Sparkify data architecture is divided into two places, the first comes from the app and contains, among other things, user data and the second is the Sparkify database that has the music data. A simple query takes a lot of time to analyze and study the analyst in the data.
With the new architecture, everything is easier, because the data is clearly separated in the dimension tables, and depending on the query he wants he can use a simple query in the fact table.


### Purpose of this database.

This database has the purpose of organizing the data in a single database, making the data more accessible and easy to manage.


In the case of the Sparkify application, the relational database was used in a hybrid way, with standardized tables that dimension tables that generate a facts table.

                                                                                                                                                                 
### Design justification.

The fact table has data that belongs to other tables, that is, it is duplicate data, and this table possibly will be out of date, but the benefit is that with a simple consultation the fact table we can obtain the data of the songs played.


