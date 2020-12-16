### Purpose of this database.

This database has the purpose of organizing the data in a single database, making the data more accessible and easy to manage.


In the case of the Sparkify application, the relational database was used in a hybrid way, with standardized tables that dimension tables that generate a facts table.

                                                                                                                                                                 
### Design justification.

The fact table has data that belongs to other tables, that is, it is duplicate data, and this table possibly will be out of date, but the benefit is that with a simple consultation the fact table we can obtain the data of the songs played.


