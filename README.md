# Converts Lucene Query DSL to SQL
Uses Parslet gem to describe source DSL grammar and convert the input string to SQL.

Sample use:
```
sample_query = '(field1:"value one" AND field2:"value two") OR (field3:[4;10])'
puts LuceneToSql::Converter.new(sample_query).sql

# Outputs the following SQL:
# (field1 = 'value one' AND field2 = 'value two') OR (field3 BETWEEN '4' AND '10')
```