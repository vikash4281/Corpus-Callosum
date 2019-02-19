## Corpus Callosum

Lets take Facebook: They have an OLTP database with tables for users, groups, events, posts etc. They want to run analytics on those (how many users are active, avg number of groups a user joins etc). If they run this on the OLTP database, then the user experience (creating new users, creting new groups, joining a group) would be negatively impacted. In addition, the analysis queries might be slow themselves. So, syncing it with an OLAP database and moving analytics queries there allows data scientists to run their queries swiftly without affecting user experience.

### Project Idea
Synchronizing OLTP with OLAP Database in real time.

### Tech Stack

- PostgreSQL
- Kafka 
- RedShift



### Data Source

GDELT News Data.

### Engineering Challenge

- Performing Joins on Streaming Data that comes at different frequencies.
- Modelling data according to the OLAP schema in real time.

### Business Value

Reduces the time consumed for running analytical queries on large databases.
