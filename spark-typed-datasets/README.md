# spark-typed-datasets

An IntelliJ IDEA + Scala + SBT project demonstrating Spark DataFrames (untyped API)
and typed `Dataset[Employee]` operations.

## Project structure

```text
spark-typed-datasets/
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── com/
                └── example/
                    └── TypedDatasetDemo.scala
```

## Version baseline

- Scala: 2.12.18
- Apache Spark SQL: 3.5.3
- SBT: 1.10.7
- JDK: 8, 11, or 17 (use a version supported by your environment)

## Open in IntelliJ IDEA

1. Extract the ZIP file.
2. Open the `spark-typed-datasets` folder in IntelliJ IDEA.
3. Make sure the Scala plugin is installed.
4. Allow IntelliJ to import/reload the SBT project.
5. Wait for Spark dependencies to download.
6. Run `com.example.TypedDatasetDemo`.

## Run from a terminal

Open a terminal in the project root and run:

```bash
sbt clean
sbt compile
sbt run
```

The first run may take several minutes because SBT downloads Spark and its dependencies.

## What the application demonstrates

1. Creates an in-memory sequence of employee case-class records.
2. Converts the sequence to an untyped DataFrame using `toDF()`.
3. Filters and selects columns with Spark column expressions.
4. Converts the same sequence to `Dataset[Employee]` using `toDS()`.
5. Filters typed employee objects using case-class fields.
6. Maps the typed Dataset to `EmployeePayView`.
7. Maps the typed Dataset to `EmployeeSummary`, calculating monthly salary.

## Expected filtered employees

Both the untyped and typed filters should retain:

- Meera — Engineering — 98000.0
- Asha — Engineering — 90000.0

The summary output includes all four employees, with monthly salary calculated as
annual salary divided by 12.

## Important

`DataFrame` is an alias for `Dataset[Row]`. A typed `Dataset[Employee]` uses an
Encoder to convert between JVM Employee objects and Spark SQL's internal
representation. Typed APIs provide convenient compile-time checking for
case-class field access, but they are not automatically faster than DataFrame
column expressions.
