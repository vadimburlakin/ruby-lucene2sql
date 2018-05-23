require 'parslet'
require 'mysql'

module LuceneToSql

    class QueryParser < Parslet::Parser
        def stri(str)
            key_chars = str.split(//)
            key_chars.collect! { |char| match["#{char.upcase}#{char.downcase}"] }.reduce(:>>)
        end

        # atoms
        rule(:lparen)          { str('(') }
        rule(:rparen)          { str(')') }
        rule(:space)           { match('\s').repeat(1) }
        rule(:space?)          { space.maybe }
        rule(:quote)           { str('"') }
        rule(:nonquote)        { str('"').absnt? >> any }
        rule(:colon)           { str(':') }
        rule(:colongt)         { str(':>') }
        rule(:colonlt)         { str(':<') }
        rule(:atom_operator)   { colon | colongt | colonlt }
        rule(:semicolon)       { str(';') }
        rule(:escape)          { str('\\') >> any }
        rule(:quoted_string)   { quote >> (escape | nonquote).repeat(1) >> quote }
        rule(:unquoted_string) { match('[^\s\)\(\[\])]').repeat(1) }
        rule(:range_value)     { match('[^\s\[\]\(\);]').repeat(1) }
        rule(:operator)        { stri('OR') | stri('ANDNOT') | stri('AND') }
        rule(:value)           { range.as(:range) | quoted_string.as(:quoted_string) | unquoted_string.as(:unquoted_string) | number.as(:number)

                               }
        rule(:number)          { match('[\d]').repeat(1) >> (str('.') >> match('[\d]').repeat).maybe }
        rule(:field)           { match('\w').repeat(1,1) >> match('[\w\d]').repeat }

        rule(:range)           { str('[') >> range_value.as(:left_value) >> str(';') >>
                                 range_value.as(:right_value) >> str(']')
                               }
        rule(:lucene_atom)     { (field.as(:field) >> atom_operator.as(:atom_operator) >> value.as(:value)).as(:lucene_atom) }

        # grammar
        rule(:expressions)     { expression >> (space >> operator.as(:operator).repeat(1,1) >> space >> expression).repeat }
        rule(:expression)      { lucene_atom | (lparen >> expressions >> rparen).as(:group) }

        # query types
        rule(:lucene_query)    { expressions | expression }

        root(:lucene_query)
    end

    class ParseTreeNormalizer < Parslet::Transform
        rule(:lucene_atom => subtree(:lucene_atom)) {
            atom_operators = {':' => '=', ':>' => '>', ':<' => '<'}

            field    = lucene_atom[:field].to_s
            operator = atom_operators[lucene_atom[:atom_operator].to_s]
            operator = lucene_atom[:value][:new_operator] if lucene_atom[:value][:new_operator]
            value    = lucene_atom[:value][:value]

            raise "Unknown operator: #{lucene_atom[:atom_operator]}" if operator.nil?

            {type: :atom, data: {field: field, operator: operator, value: value} }
        }

        rule(:quoted_string => simple(:x)) {
            value = x.to_s
            value.gsub!(/^"(.+)"$/) { $1 }
            value.gsub!(/\\(.{1})/) { $1 }
            {is_range: false, new_operator:nil, value: [value]}
        }

        rule(:unquoted_string => simple(:x)) {
            value = x.to_s
            value.gsub!(/\\(.{1})/) { $1 }
            {is_range: false, new_operator:nil, value: [value]}
        }

        rule(:number => simple(:x)) {
            value = x.to_s
            {is_range: false, new_operator:nil, value: [value]}
        }

        rule(:range => subtree(:x)) {
            left_value  = x[:left_value].to_s
            right_value = x[:right_value].to_s
            left_value.gsub!(/\\(.{1})/) { $1 }
            right_value.gsub!(/\\(.{1})/) { $1 }

            raise 'Both values of range query cannot be *' if left_value == '*' and right_value == '*'

            r = {is_range: false, new_operator: '<', value: [right_value]} if left_value  == '*'
            r = {is_range: false, new_operator: '>', value: [left_value] } if right_value == '*'
            r = {is_range: true,  new_operator: 'BETWEEN', value: [left_value, right_value]} if r.nil?

            r
        }

        rule(:group => subtree(:x)) {
            { type: :group, data: x }
        }

        rule(:operator => simple(:operator)) {
            lucene_operators = {'and' => 'AND', 'or' => 'OR', 'andnot' => 'AND NOT', 'not' => 'NOT'}
            transformed_operator = lucene_operators[operator.to_s.downcase]

            raise "Unknown operator: #{operator}" if transformed_operator.nil?

            { type: :operator, data: transformed_operator }
        }

        def normalize(parse_tree)
            tree = apply(parse_tree)
            tree = [tree] if tree.is_a?(Hash)
            tree.map {|e| normalize_node(e) }
            return tree
        end

        private

        def normalize_node(node)
            node[:data] = [node[:data]] if node[:type]==:group and node[:data].is_a?(Hash)
            if node[:type]==:group and node[:data].is_a?(Array)
                node[:data].map { |e| (e[:type]==:group) ? normalize_node(e) : e }
            end
            return node
        end

    end

    class SqlTransformer
        attr_accessor :tree, :table

        def initialize(tree)
            @tree  = tree
        end

        def tree=(value)
            @tree = value
        end

        def sql
            output = []
            tree.each { |node| output << render_node(node) }
            return output.join(' ')
        end

        private

        def render_node(node)
            return render_atom(node[:data])      if node[:type] == :atom
            return render_operator(node[:data])  if node[:type] == :operator
            return render_group(node[:data])     if node[:type] == :group
        end

        def render_value(field,value)
            '\'' + Mysql::escape_string(value) + '\''
        end

        def render_group(data)
            output = []
            data.each { |node| output << render_node(node) }
            '(' + output.join(' ') + ')'
        end

        def render_operator(operator)
            operator
        end

        def render_atom(data)
            field      = data[:field]
            operator   = data[:operator]
            value      = data[:value]
            output     = []

            output << field

            case operator
            when '<','>','='
                output << operator
                output << render_value(field,value[0])
            when 'BETWEEN'
                output << 'BETWEEN'
                output << render_value(field,value[0])
                output << 'AND'
                output << render_value(field,value[1])
            else
                raise "Don't know how to render operator: #{operator}"
            end

            return output.join(' ')
        end
    end

    class Converter
        attr_accessor :query
        attr_accessor :query_tree

        def initialize(query)
            self.query = query
        end

        def query=(query)
            @query = query
            @query_tree = ParseTreeNormalizer.new.normalize(QueryParser.new.parse(@query)) if query != ''
        end

        def sql
            return '' if query == ''
            return SqlTransformer.new(query_tree).sql
        end
    end
end