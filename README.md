
<p align="center">
  <img src="https://raw.githubusercontent.com/inferlabs/dbt-infer/v1.2.0/lockup_black.png" alt="Infer logo" width="350" style="margin-right: 50px"/>
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>
<p align="center">
  <a href="https://github.com/inferlabs/dbt-infer/actions/workflows/main.yml">
    <img src="https://github.com/inferlabs/dbt-infer/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
</p>

# dbt-infer

The [dbt](https://www.getdbt.com) adapter for [Infer](https://www.getinfer.io/).
`dbt-infer` allows you to connect your dbt instance to Infer and use **SQL-inf** syntax in your dbt models.

`dbt-infer` acts as a layer between your existing data warehouse and your dbt instance, enabling you to perform
ML analytics within your dbt models.

`dbt-infer` packages are hosted on [PyPi](https://github.com/inferlabs/dbt-infer).

## SQL-inf

SQL-inf is an extension of SQL that introduces machine learning primitives to SQL.
These primitives can be used within any part of your SQL queries, or DBT models, and allow you to build ML
analytics use cases.

Read more about SQL-inf [here](https://docs.getinfer.io/docs/reference).

### Examples

Illustrative example based on an idealised table `users` with some simple demographic data, whether the user has
churned or not, their lifetime value(LTV) and a text field with customer feedback.

More examples and tutorials available [here](https://docs.getinfer.io/docs/tutorial/intro).

Predict column `has_churn` from the other columns in the table `users`.
```sql
SELECT * FROM users PREDICT(has_churned)
```

Understand what columns drive values of `has_churned`
```sql
SELECT * FROM users EXPLAIN(PREDICT(has_churned))
```

Predict and understand the LTV in column `ltv` from table `users`.
```sql
SELECT * FROM users PREDICT(ltv)
```

```sql
SELECT * FROM users EXPLAIN(PREDICT(ltv))
```

Perform text analysis, sentiment and topic analysis, on user feedback

```sql
SELECT * FROM users SENTIMENT(feedback)
```

```sql
SELECT * FROM users TOPICS(feedback)
```

Create user segmentations on demographics

```sql
SELECT age, location, gender, job, education FROM users CLUSTER()
```

Find the sizes of the user segmentations

```sql
SELECT cluster_id, COUNT(*) as size FROM (
    SELECT age, location, gender, job, education FROM users CLUSTER()
) GROUP BY cluster_id
```


Find users similar to the user with `user_id=123`

```sql
SELECT * FROM users SIMILAR_TO(user_id=123)
```

Analyse what, if any, demographic features drive feedback sentiment
```sql
SELECT age, location, gender, job, education, prediction FROM (
    SELECT * FROM users SENTIMENT(feedback)
) EXPLAIN(PREDICT(prediction))
```


## Installation

Detailed installation and setup guide [here](https://dbt.getinfer.io/docs/getting_started).

### Requirements

You should be using dbt 1.2 or later.

### Setup Infer account

First you need to setup your Infer account and generate your api key.

Read about how to do that [here](https://docs.getinfer.io/docs/reference/api).

### Install `dbt-infer`
```shell
pip install dbt-infer
```

### Setting up dbt-infer

Setup a target in your profile for `dbt-infer` with the following shape
```yaml
<target_name>:
  url: <infer-api-endpoint>
  username: <infer-api-username>
  apikey: <infer-apikey>
  type: infer
  data_config:
    <here goes your normal data warehouse config>
```
where `data_config` contains the profile settings for your underlying data warehouse.
