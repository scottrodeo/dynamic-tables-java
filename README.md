# Dynamic Tables (Java)

A lightweight Java library for dynamically creating and managing PostgreSQL tables based on input data at runtime.

---

## 🚀 Features

- Dynamically generates PostgreSQL tables from runtime input.
- Customizable naming conventions and schemas.
- Includes helper methods for connecting, inserting, querying, and deleting.
- Clean, extensible design—ready for integration into larger systems.

---

## 📦 Installation

### Install via GitHub (Manual Method)

Since this package isn't on Maven Central yet, you can build and use it locally:

1. Clone the repository:

    git clone https://github.com/scottrodeo/dynamic-tables-java.git
    cd dynamic-tables-java

2. Build the project:

    ./gradlew build

3. Use the generated JAR:

The compiled JAR will be located in:

    build/libs/dynamic-tables-1.0.0.jar

You can import this into your project manually or install it to your local Maven repository:

    ./gradlew publishToMavenLocal

---

## 📌 Example Usage

    DynamicTables dt = new DynamicTables();
    dt.setTablePrefix("dyn_");
    dt.setColumns("domain TEXT, category TEXT, lang TEXT");
    dt.setDynamicColumn("domain");

    // Insert dynamic rows
    dt.input("wikipedia.org", "cats", "en");
    dt.input("wikipedia.org", "dogs", "en");

    // Show created tables
    dt.showTables();

---

## 🛠️ Available Methods

| Method | Description |
|--------|-------------|
| setColumns(String columnSpec) | Define schema for dynamic tables |
| setTablePrefix(String prefix) | Set prefix for all dynamic tables |
| setDynamicColumn(String column) | Use column value as unique table key |
| input(Object... values) | Insert a new row, creating table if needed |
| showTables() | List all dynamically created tables |
| getColumns(String tableName) | Get column definitions for a table |
| selectTable(String tableName) | Retrieve all rows from a table |
| deleteTables() | Drop all dynamically created tables |
| connect(String db, String user, String pass, String host) | Connect to PostgreSQL |
| close() | Close the database connection |
| status() | Print current connection and config status |
| logLevel(String level) | Adjust logging verbosity |

---

## 🧪 Testing

To run tests:

    ./gradlew test

---

## 🤝 Contributing

Contributions are welcome!  
To contribute:

1. Fork this repo.
2. Create a feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m "Add feature"`).
4. Push to GitHub (`git push origin feature-name`).
5. Open a pull request.

---

## 📄 License

This project is licensed under the Apache License 2.0.  
See the LICENSE file for details.

---

## 🌐 Links

- GitHub Repository: https://github.com/scottrodeo/dynamic-tables-java
- Issue Tracker: https://github.com/scottrodeo/dynamic-tables-java/issues

---

🚀 Happy Coding!

