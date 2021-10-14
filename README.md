# kafka-connect-jsonpath-accessor
A library enables to access Kafka connect data with Json Path expression.
You can access Kafka connect data using expressions such as JSON Path `$.name`.

This library is intended to help data manipulation in Kafka connect or SMT application.
The currently supported connect data types are `org.apache.kafka.connect.data.Struct` and `Map<String, Object>`.

## Getting started
Select latest version from [the maven repository](https://mvnrepository.com/artifact/io.github.rerorero/kafka-connect-jsonpath-accessor).

## Usage
Example Code
```java
HashMap<String, Object> data = new HashMap<>();
data.put("users", Arrays.asList(
        new HashMap<String, Object>() {{
            put("name", "Fifer");
        }},
        new HashMap<String, Object>() {{
            put("name", "Fiddler");
        }},
        new HashMap<String, Object>() {{
            put("name", "Practical Guy");
        }}
));

// Getter/Updater instance is stateless. You can call `run` against different data
MapAccessor.Getter getter = new MapAccessor.Getter("$.users[*].name");

Map<String, Object> values = getter.run(data);

values.forEach((k, v) -> System.out.println((k + ":" + v)));
// $.users[0].name:Fifer
// $.users[1].name:Fiddler
// $.users[2].name:Practical Guy


MapAccessor.Updater updater = new MapAccessor.Updater("$.users[*].name");
Map<String, Object> updated = updater.run(data, new HashMap<String, Object>(){{
    put("$.users[0].name", "Ultimate Guy");
    put("$.users[1].name", "Incredible Guy");
}});

System.out.println(updated);
// {users=[{name=Ultimate Guy}, {name=Incredible Guy}, {name=Practical Guy}]}
```

## Json Path Expressions

Only some expressions are supported, as follows:

| Operator     | Description                                                                 |
| ------------ | --------------------------------------------------------------------------- |
| `$`          | The root element. All JsonPath string has to be started with this operator. |
| `*`          | Wildcard. Only supported for use as an array index.                         |
| `.<name>`    | Dot-notated child.                                                          |
| `['name']`   | Bracket-notated child. Multiple names are not supported.                    |
| `[<number>]` | Array index. Multiple indices are not supported.                            |
