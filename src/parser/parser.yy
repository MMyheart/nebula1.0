%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { GraphParser }
%lex-param { nebula::GraphScanner& scanner }
%parse-param { nebula::GraphScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::SequentialSentences** sentences }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include <cstddef>
#include "parser/SequentialSentences.h"

namespace nebula {

class GraphScanner;

}

static constexpr size_t MAX_ABS_INTEGER = 9223372036854775808ULL;

}

%code {
    static int yylex(nebula::GraphParser::semantic_type* yylval,
                     nebula::GraphParser::location_type *yylloc,
                     nebula::GraphScanner& scanner);

    void ifOutOfRange(const int64_t input,
                      const nebula::GraphParser::location_type& loc);
}

%union {
    bool                                    boolval;
    int64_t                                 intval;
    double                                  doubleval;
    std::string                            *strval;
    nebula::Expression                     *expr;
    nebula::Sentence                       *sentence;
    nebula::SequentialSentences            *sentences;
    nebula::ColumnSpecification            *colspec;
    nebula::ColumnSpecificationList        *colspeclist;
    nebula::ColumnNameList                 *colsnamelist;
    nebula::ColumnType                      type;
    nebula::StepClause                     *step_clause;
    nebula::StepClause                     *find_path_upto_clause;
    nebula::FromClause                     *from_clause;
    nebula::ToClause                       *to_clause;
    nebula::VertexIDList                   *vid_list;
    nebula::OverEdge                       *over_edge;
    nebula::OverEdges                      *over_edges;
    nebula::FetchLabels                    *fetch_labels;
    nebula::SampleLabels                   *sample_labels;
    nebula::OverClause                     *over_clause;
    nebula::WhereClause                    *where_clause;
    nebula::WhenClause                     *when_clause;
    nebula::YieldClause                    *yield_clause;
    nebula::YieldColumns                   *yield_columns;
    nebula::YieldColumn                    *yield_column;
    nebula::VertexTagList                  *vertex_tag_list;
    nebula::VertexTagItem                  *vertex_tag_item;
    nebula::PropertyList                   *prop_list;
    nebula::ValueList                      *value_list;
    nebula::VertexRowList                  *vertex_row_list;
    nebula::VertexRowItem                  *vertex_row_item;
    nebula::EdgeRowList                    *edge_row_list;
    nebula::EdgeRowItem                    *edge_row_item;
    nebula::UpdateList                     *update_list;
    nebula::UpdateItem                     *update_item;
    nebula::ArgumentList                   *argument_list;
    nebula::SpaceOptList                   *space_opt_list;
    nebula::SpaceOptItem                   *space_opt_item;
    nebula::AlterSchemaOptList             *alter_schema_opt_list;
    nebula::AlterSchemaOptItem             *alter_schema_opt_item;
    nebula::RoleTypeClause                 *role_type_clause;
    nebula::AclItemClause                  *acl_item_clause;
    nebula::SchemaPropList                 *create_schema_prop_list;
    nebula::SchemaPropItem                 *create_schema_prop_item;
    nebula::SchemaPropList                 *alter_schema_prop_list;
    nebula::SchemaPropItem                 *alter_schema_prop_item;
    nebula::OrderFactor                    *order_factor;
    nebula::OrderFactors                   *order_factors;
    nebula::ConfigModule                    config_module;
    nebula::ConfigRowItem                  *config_row_item;
    nebula::EdgeKey                        *edge_key;
    nebula::EdgeKeys                       *edge_keys;
    nebula::EdgeKeyRef                     *edge_key_ref;
    nebula::GroupClause                    *group_clause;
    nebula::HostList                       *host_list;
    nebula::HostAddr                       *host_item;
    std::vector<int32_t>                   *integer_list;
}

/* destructors */
%destructor {} <sentences>
%destructor {} <boolval> <intval> <doubleval> <type> <config_module> <integer_list>
%destructor { delete $$; } <*>

/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_AND KW_XOR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_YIELD KW_RETURN KW_CREATE KW_VERTEX KW_OFFLINE
%token KW_EDGE KW_EDGES KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_SPACE KW_DELETE KW_FIND KW_REBUILD
%token KW_INT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_TAGS KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN KW_DESCRIBE KW_DESC KW_SHOW KW_HOSTS KW_PART KW_PARTS KW_TIMESTAMP KW_ADD
%token KW_PARTITION_NUM KW_REPLICA_FACTOR KW_CHARSET KW_COLLATE KW_COLLATION
%token KW_DROP KW_REMOVE KW_SPACES KW_INGEST KW_INDEX KW_INDEXES
%token KW_IF KW_NOT KW_EXISTS KW_WITH
%token KW_COUNT KW_COUNT_DISTINCT KW_SUM KW_AVG KW_MAX KW_MIN KW_STD KW_BIT_AND KW_BIT_OR KW_BIT_XOR
%token KW_BY KW_DOWNLOAD KW_HDFS KW_UUID KW_CONFIGS KW_FORCE KW_STATUS
%token KW_GET KW_DECLARE KW_GRAPH KW_META KW_STORAGE
%token KW_TTL KW_TTL_DURATION KW_TTL_COL KW_DATA KW_STOP
%token KW_FETCH KW_PROP KW_UPDATE KW_UPSERT KW_WHEN
%token KW_SAMPLE
%token KW_ORDER KW_ASC KW_LIMIT KW_OFFSET KW_GROUP
%token KW_DISTINCT KW_ALL KW_OF
%token KW_BALANCE KW_LEADER
%token KW_SHORTEST KW_PATH KW_NOLOOP
%token KW_IS KW_NULL KW_DEFAULT
%token KW_SNAPSHOT KW_SNAPSHOTS KW_LOOKUP
%token KW_SCAN
%token KW_JOBS KW_JOB KW_RECOVER KW_FLUSH KW_COMPACT KW_SUBMIT
%token KW_BIDIRECT
%token KW_USER KW_USERS KW_ACCOUNT
%token KW_PASSWORD KW_CHANGE KW_ROLE KW_ROLES
%token KW_GOD KW_ADMIN KW_DBA KW_GUEST KW_GRANT KW_REVOKE KW_ON
%token KW_CONTAINS
%token KW_SAMPLENB KW_RANDOMWALK

/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND XOR LT LE GT GE EQ NE PLUS MINUS MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON L_ARROW R_ARROW AT
%token ID_PROP TYPE_PROP SRC_ID_PROP DST_ID_PROP RANK_PROP INPUT_REF DST_REF SRC_REF

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER IPV4
%token <doubleval> DOUBLE
%token <strval> STRING VARIABLE LABEL

%type <strval> name_label unreserved_keyword agg_function
%type <strval> admin_operation admin_para
%type <expr> expression logic_xor_expression logic_or_expression logic_and_expression
%type <expr> relational_expression multiplicative_expression additive_expression arithmetic_xor_expression
%type <expr> unary_expression primary_expression equality_expression base_expression
%type <expr> src_ref_expression
%type <expr> dst_ref_expression
%type <expr> input_ref_expression
%type <expr> var_ref_expression
%type <expr> alias_ref_expression
%type <expr> vid_ref_expression
%type <expr> vid
%type <expr> function_call_expression
%type <expr> uuid_expression
%type <expr> scan_part_clause scan_from_clause scan_limit_clause
%type <argument_list> argument_list opt_argument_list
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <vid_list> vid_list
%type <over_edge> over_edge
%type <over_edges> over_edges
%type <fetch_labels> fetch_labels
%type <sample_labels> sample_labels
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <when_clause> when_clause
%type <yield_clause> yield_clause
%type <yield_columns> yield_columns
%type <yield_column> yield_column
%type <vertex_tag_list> vertex_tag_list
%type <vertex_tag_item> vertex_tag_item
%type <prop_list> prop_list
%type <value_list> value_list
%type <vertex_row_list> vertex_row_list
%type <vertex_row_item> vertex_row_item
%type <edge_row_list> edge_row_list
%type <edge_row_item> edge_row_item
%type <update_list> update_list
%type <update_item> update_item
%type <space_opt_list> space_opt_list
%type <space_opt_item> space_opt_item
%type <alter_schema_opt_list> alter_schema_opt_list
%type <alter_schema_opt_item> alter_schema_opt_item
%type <create_schema_prop_list> create_schema_prop_list opt_create_schema_prop_list
%type <create_schema_prop_item> create_schema_prop_item
%type <alter_schema_prop_list> alter_schema_prop_list
%type <alter_schema_prop_item> alter_schema_prop_item
%type <order_factor> order_factor
%type <order_factors> order_factors
%type <config_module> config_module_enum
%type <config_row_item> show_config_item get_config_item set_config_item
%type <edge_key> edge_key
%type <edge_keys> edge_keys
%type <edge_key_ref> edge_key_ref
%type <to_clause> to_clause
%type <find_path_upto_clause> find_path_upto_clause
%type <group_clause> group_clause
%type <host_list> host_list
%type <host_item> host_item
%type <integer_list> integer_list

%type <intval> unary_integer rank port

%type <colspec> column_spec
%type <colspeclist> column_spec_list
%type <colsnamelist> column_name_list

%type <role_type_clause> role_type_clause
%type <acl_item_clause> acl_item_clause

%type <sentence> maintain_sentence
%type <sentence> create_space_sentence describe_space_sentence drop_space_sentence
%type <sentence> create_tag_sentence create_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> drop_tag_sentence drop_edge_sentence
%type <sentence> describe_tag_sentence describe_edge_sentence
%type <sentence> create_tag_index_sentence create_edge_index_sentence
%type <sentence> drop_tag_index_sentence drop_edge_index_sentence
%type <sentence> describe_tag_index_sentence describe_edge_index_sentence
%type <sentence> rebuild_tag_index_sentence rebuild_edge_index_sentence
%type <sentence> rebuild_sample_sentence
%type <sentence> create_snapshot_sentence drop_snapshot_sentence

%type <sentence> admin_sentence
%type <sentence> create_user_sentence alter_user_sentence drop_user_sentence change_password_sentence
%type <sentence> show_sentence

%type <sentence> mutate_sentence
%type <sentence> insert_vertex_sentence insert_edge_sentence
%type <sentence> delete_vertex_sentence delete_edge_sentence
%type <sentence> update_vertex_sentence update_edge_sentence
%type <sentence> download_sentence ingest_sentence

%type <sentence> traverse_sentence
%type <sentence> go_sentence match_sentence lookup_sentence find_path_sentence sample_nb_sentence random_walk_sentence
%type <sentence> scan_sentence
%type <sentence> group_by_sentence order_by_sentence limit_sentence
%type <sentence> fetch_sentence fetch_vertices_sentence fetch_edges_sentence
%type <sentence> set_sentence piped_sentence assignment_sentence
%type <sentence> yield_sentence use_sentence
%type <sentence> sample_sentence

%type <sentence> grant_sentence revoke_sentence
%type <sentence> set_config_sentence get_config_sentence balance_sentence
%type <sentence> process_control_sentence return_sentence
%type <sentence> sentence
%type <sentences> sentences

%type <boolval> opt_if_not_exists
%type <boolval> opt_if_exists


%start sentences

%%

name_label
     : LABEL { $$ = $1; }
     | unreserved_keyword { $$ = $1; }
     ;

unreserved_keyword
     : KW_SPACE              { $$ = new std::string("space"); }
     | KW_VALUES             { $$ = new std::string("values"); }
     | KW_HOSTS              { $$ = new std::string("hosts"); }
     | KW_SPACES             { $$ = new std::string("spaces"); }
     | KW_USER               { $$ = new std::string("user"); }
     | KW_USERS              { $$ = new std::string("users"); }
     | KW_PASSWORD           { $$ = new std::string("password"); }
     | KW_ROLE               { $$ = new std::string("role"); }
     | KW_ROLES              { $$ = new std::string("roles"); }
     | KW_GOD                { $$ = new std::string("god"); }
     | KW_ADMIN              { $$ = new std::string("admin"); }
     | KW_DBA                { $$ = new std::string("dba"); }
     | KW_GUEST              { $$ = new std::string("guest"); }
     | KW_GROUP              { $$ = new std::string("group"); }
     | KW_COUNT              { $$ = new std::string("count"); }
     | KW_SUM                { $$ = new std::string("sum"); }
     | KW_AVG                { $$ = new std::string("avg"); }
     | KW_MAX                { $$ = new std::string("max"); }
     | KW_MIN                { $$ = new std::string("min"); }
     | KW_STD                { $$ = new std::string("std"); }
     | KW_BIT_AND            { $$ = new std::string("bit_and"); }
     | KW_BIT_OR             { $$ = new std::string("bit_or"); }
     | KW_BIT_XOR            { $$ = new std::string("bit_xor"); }
     | KW_PATH               { $$ = new std::string("path"); }
     | KW_DATA               { $$ = new std::string("data"); }
     | KW_LEADER             { $$ = new std::string("leader"); }
     | KW_UUID               { $$ = new std::string("uuid"); }
     | KW_JOB                { $$ = new std::string("job"); }
     | KW_JOBS               { $$ = new std::string("jobs"); }
     | KW_BIDIRECT           { $$ = new std::string("bidirect"); }
     | KW_OFFLINE            { $$ = new std::string("offline"); }
     | KW_FORCE              { $$ = new std::string("force"); }
     | KW_STATUS             { $$ = new std::string("status"); }
     | KW_PART               { $$ = new std::string("part"); }
     | KW_PARTS              { $$ = new std::string("parts"); }
     | KW_DEFAULT            { $$ = new std::string("default"); }
     | KW_CONFIGS            { $$ = new std::string("configs"); }
     | KW_ACCOUNT            { $$ = new std::string("account"); }
     | KW_HDFS               { $$ = new std::string("hdfs"); }
     | KW_PARTITION_NUM      { $$ = new std::string("partition_num"); }
     | KW_REPLICA_FACTOR     { $$ = new std::string("replica_factor"); }
     | KW_CHARSET            { $$ = new std::string("charset"); }
     | KW_COLLATE            { $$ = new std::string("collate"); }
     | KW_COLLATION          { $$ = new std::string("collation"); }
     | KW_TTL_DURATION       { $$ = new std::string("ttl_duration"); }
     | KW_TTL_COL            { $$ = new std::string("ttl_col"); }
     | KW_SNAPSHOT           { $$ = new std::string("snapshot"); }
     | KW_SNAPSHOTS          { $$ = new std::string("snapshots"); }
     | KW_GRAPH              { $$ = new std::string("graph"); }
     | KW_META               { $$ = new std::string("meta"); }
     | KW_STORAGE            { $$ = new std::string("storage"); }
     | KW_ALL                { $$ = new std::string("all"); }
     | KW_SHORTEST           { $$ = new std::string("shortest"); }
     | KW_NOLOOP             { $$ = new std::string("noloop"); }
     | KW_COUNT_DISTINCT     { $$ = new std::string("count_distinct"); }
     | KW_CONTAINS           { $$ = new std::string("contains"); }
     ;

agg_function
     : KW_COUNT              { $$ = new std::string("COUNT"); }
     | KW_COUNT_DISTINCT     { $$ = new std::string("COUNT_DISTINCT"); }
     | KW_SUM                { $$ = new std::string("SUM"); }
     | KW_AVG                { $$ = new std::string("AVG"); }
     | KW_MAX                { $$ = new std::string("MAX"); }
     | KW_MIN                { $$ = new std::string("MIN"); }
     | KW_STD                { $$ = new std::string("STD"); }
     | KW_BIT_AND            { $$ = new std::string("BIT_AND"); }
     | KW_BIT_OR             { $$ = new std::string("BIT_OR"); }
     | KW_BIT_XOR            { $$ = new std::string("BIT_XOR"); }
     ;

primary_expression
    : INTEGER {
        ifOutOfRange($1, @1);
        $$ = new PrimaryExpression($1);
    }
    | MINUS INTEGER {
        $$ = new PrimaryExpression(-$2);;
    }
    | MINUS base_expression {
        $$ = new UnaryExpression(UnaryExpression::NEGATE, $2);
    }
    | base_expression {
        $$ = $1;
    }

base_expression
    : DOUBLE {
        $$ = new PrimaryExpression($1);
    }
    | STRING {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | BOOL {
        $$ = new PrimaryExpression($1);
    }
    | name_label {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | input_ref_expression {
        $$ = $1;
    }
    | src_ref_expression {
        $$ = $1;
    }
    | dst_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    | alias_ref_expression {
        $$ = $1;
    }
    | L_PAREN expression R_PAREN {
        $$ = $2;
    }
    | function_call_expression {
        $$ = $1;
    }
    ;

input_ref_expression
    : INPUT_REF DOT name_label {
        $$ = new InputPropertyExpression($3);
    }
    | INPUT_REF {
        // To reference the `id' column implicitly
        $$ = new InputPropertyExpression(new std::string("id"));
    }
    | INPUT_REF DOT MUL {
        $$ = new InputPropertyExpression(new std::string("*"));
    }
    ;

src_ref_expression
    : SRC_REF DOT name_label DOT name_label {
        $$ = new SourcePropertyExpression($3, $5);
    }
    ;

dst_ref_expression
    : DST_REF DOT name_label DOT name_label {
        $$ = new DestPropertyExpression($3, $5);
    }
    ;

var_ref_expression
    : VARIABLE DOT name_label {
        $$ = new VariablePropertyExpression($1, $3);
    }
    | VARIABLE {
        $$ = new VariablePropertyExpression($1, new std::string("id"));
    }
    | VARIABLE DOT MUL {
        $$ = new VariablePropertyExpression($1, new std::string("*"));
    }
    ;

alias_ref_expression
    : name_label DOT name_label {
        $$ = new AliasPropertyExpression(new std::string(""), $1, $3);
    }
    | name_label DOT TYPE_PROP {
        $$ = new EdgeTypeExpression($1);
    }
    | name_label DOT SRC_ID_PROP {
        $$ = new EdgeSrcIdExpression($1);
    }
    | name_label DOT DST_ID_PROP {
        $$ = new EdgeDstIdExpression($1);
    }
    | name_label DOT RANK_PROP {
        $$ = new EdgeRankExpression($1);
    }
    ;

function_call_expression
    : LABEL L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression($1, $3);
    }
    ;

uuid_expression
    : KW_UUID L_PAREN STRING R_PAREN {
        $$ = new UUIDExpression($3);
    }
    ;

opt_argument_list
    : %empty {
        $$ = nullptr;
    }
    | argument_list {
        $$ = $1;
    }
    ;

argument_list
    : expression {
        $$ = new ArgumentList();
        $$->addArgument($1);
    }
    | argument_list COMMA expression {
        $$ = $1;
        $$->addArgument($3);
    }
    ;

unary_expression
    : primary_expression { $$ = $1; }
    | PLUS unary_expression {
        $$ = new UnaryExpression(UnaryExpression::PLUS, $2);
    }
    | NOT unary_expression {
        $$ = new UnaryExpression(UnaryExpression::NOT, $2);
    }
    | KW_NOT unary_expression {
        $$ = new UnaryExpression(UnaryExpression::NOT, $2);
    }
    | L_PAREN type_spec R_PAREN unary_expression {
        $$ = new TypeCastingExpression($2, $4);
    }
    ;

type_spec
    : KW_INT { $$ = ColumnType::INT; }
    | KW_DOUBLE { $$ = ColumnType::DOUBLE; }
    | KW_STRING { $$ = ColumnType::STRING; }
    | KW_BOOL { $$ = ColumnType::BOOL; }
    | KW_TIMESTAMP { $$ = ColumnType::TIMESTAMP; }
    ;

arithmetic_xor_expression
    : unary_expression { $$ = $1; }
    | arithmetic_xor_expression XOR unary_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::XOR, $3);
    }
    ;

multiplicative_expression
    : arithmetic_xor_expression { $$ = $1; }
    | multiplicative_expression MUL arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MUL, $3);
    }
    | multiplicative_expression DIV arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::DIV, $3);
    }
    | multiplicative_expression MOD arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MOD, $3);
    }
    ;

additive_expression
    : multiplicative_expression { $$ = $1; }
    | additive_expression PLUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::ADD, $3);
    }
    | additive_expression MINUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::SUB, $3);
    }
    ;

relational_expression
    : additive_expression { $$ = $1; }
    | relational_expression LT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LT, $3);
    }
    | relational_expression GT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GT, $3);
    }
    | relational_expression LE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LE, $3);
    }
    | relational_expression GE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GE, $3);
    }
    | relational_expression KW_CONTAINS additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::CONTAINS, $3);
    }
    ;

equality_expression
    : relational_expression { $$ = $1; }
    | equality_expression EQ relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::EQ, $3);
    }
    | equality_expression NE relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::NE, $3);
    }
    ;

logic_and_expression
    : equality_expression { $$ = $1; }
    | logic_and_expression AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    | logic_and_expression KW_AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    ;

logic_or_expression
    : logic_and_expression { $$ = $1; }
    | logic_or_expression OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    | logic_or_expression KW_OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    ;

logic_xor_expression
    : logic_or_expression { $$ = $1; }
    | logic_xor_expression KW_XOR logic_or_expression {
        $$ = new LogicalExpression($1, LogicalExpression::XOR, $3);
    }
    ;

expression
    : logic_xor_expression { $$ = $1; }
    ;

go_sentence
    : KW_GO step_clause from_clause over_clause where_clause yield_clause {
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        if ($6 == nullptr) {
            auto *cols = new YieldColumns();
            for (auto e : $4->edges()) {
                if (e->isOverAll()) {
                    continue;
                }
                auto *edge  = new std::string(*e->edge());
                auto *expr  = new EdgeDstIdExpression(edge);
                auto *col   = new YieldColumn(expr);
                cols->addColumn(col);
            }
            $6 = new YieldClause(cols);
        }
        go->setYieldClause($6);
        $$ = go;
    }
    ;

sample_nb_sentence
    : KW_SAMPLENB from_clause over_clause where_clause KW_LIMIT INTEGER {
        ifOutOfRange($6, @2);
        auto sampleNB = new SampleNBSentence($6);
        sampleNB->setFromClause($2);
        sampleNB->setOverClause($3);
        sampleNB->setWhereClause($4);
        $$ = sampleNB;
    }
    ;
 
sample_sentence
    : KW_SAMPLE KW_VERTEX sample_labels KW_LIMIT INTEGER {
        ifOutOfRange($5, @2);
        $$ = new SampleSentence($3, $5, true);
    }
    | KW_SAMPLE KW_EDGE sample_labels KW_LIMIT INTEGER {
        ifOutOfRange($5, @2);
        $$ = new SampleSentence($3, $5);
    }
    ;
 
sample_labels
    : name_label {
        $$ = new SampleLabels();
        $$->addLabel($1);
    }
    | sample_labels COMMA name_label {
        $$ = $1;
        $$->addLabel($3);
    }
    ;
 
random_walk_sentence
    : KW_RANDOMWALK INTEGER from_clause over_clause where_clause {
        ifOutOfRange($2, @2);
        auto randomWalk = new RandomWalkSentence($2);
        randomWalk->setFromClause($3);
        randomWalk->setOverClause($4);
        randomWalk->setWhereClause($5);
        $$ = randomWalk;
    }
    ;
 
step_clause
    : %empty { $$ = new StepClause(); }
    | INTEGER KW_STEPS {
        ifOutOfRange($1, @1);
        $$ = new StepClause($1);
    }
    | INTEGER KW_TO INTEGER KW_STEPS {
        ifOutOfRange($1, @2);
        ifOutOfRange($3, @2);
        if ($1 > $3) {
            throw nebula::GraphParser::syntax_error(@1, "Invalid step range");
        }
        $$ = new StepClause($1, $3);
    }
    ;

from_clause
    : KW_FROM vid_list {
        $$ = new FromClause($2);
    }
    | KW_FROM vid_ref_expression {
        $$ = new FromClause($2);
    }
    ;

vid_list
    : vid {
        $$ = new VertexIDList();
        $$->add($1);
    }
    | vid_list COMMA vid {
        $$ = $1;
        $$->add($3);
    }
    ;

vid
    : unary_integer {
        $$ = new PrimaryExpression($1);
    }
    | function_call_expression {
        $$ = $1;
    }
    | uuid_expression {
        $$ = $1;
    }
    ;

unary_integer
    : PLUS INTEGER {
        ifOutOfRange($2, @2);
        $$ = $2;
    }
    | MINUS INTEGER {
        $$ = -$2;
    }
    | INTEGER {
        ifOutOfRange($1, @1);
        $$ = $1;
    }
    ;

vid_ref_expression
    : input_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    ;

over_edge
    : name_label {
        $$ = new OverEdge($1);
    }
    | name_label KW_AS name_label {
        $$ = new OverEdge($1, $3);
    }
    ;

over_edges
    : over_edge {
        auto edge = new OverEdges();
        edge->addEdge($1);
        $$ = edge;
    }
    | over_edges COMMA over_edge {
        $1->addEdge($3);
        $$ = $1;
    }
    ;

over_clause
    : KW_OVER MUL {
        auto edges = new OverEdges();
        auto s = new std::string("*");
        auto edge = new OverEdge(s, nullptr);
        edges->addEdge(edge);
        $$ = new OverClause(edges);
    }
    | KW_OVER MUL KW_REVERSELY {
        auto edges = new OverEdges();
        auto s = new std::string("*");
        auto edge = new OverEdge(s, nullptr);
        edges->addEdge(edge);
        $$ = new OverClause(edges, OverClause::Direction::kBackward);
    }
    | KW_OVER MUL KW_BIDIRECT {
        auto edges = new OverEdges();
        auto s = new std::string("*");
        auto edge = new OverEdge(s, nullptr);
        edges->addEdge(edge);
        $$ = new OverClause(edges, OverClause::Direction::kBidirect);
    }
    | KW_OVER over_edges {
        $$ = new OverClause($2);
    }
    | KW_OVER over_edges KW_REVERSELY {
        $$ = new OverClause($2, OverClause::Direction::kBackward);
    }
    | KW_OVER over_edges KW_BIDIRECT {
        $$ = new OverClause($2, OverClause::Direction::kBidirect);
    }
    ;

where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

when_clause
    : %empty { $$ = nullptr; }
    | KW_WHEN expression { $$ = new WhenClause($2); }
    ;

yield_clause
    : %empty { $$ = nullptr; }
    | KW_YIELD yield_columns { $$ = new YieldClause($2); }
    | KW_YIELD KW_DISTINCT yield_columns { $$ = new YieldClause($3, true); }
    ;

yield_columns
    : yield_column {
        auto fields = new YieldColumns();
        fields->addColumn($1);
        $$ = fields;
    }
    | yield_columns COMMA yield_column {
        $1->addColumn($3);
        $$ = $1;
    }
    ;

yield_column
    : expression {
        $$ = new YieldColumn($1);
    }
    | agg_function L_PAREN expression R_PAREN {
        auto yield = new YieldColumn($3);
        yield->setFunction($1);
        $$ = yield;
    }
    | expression KW_AS name_label {
        $$ = new YieldColumn($1, $3);
    }
    | agg_function L_PAREN expression R_PAREN KW_AS name_label {
        auto yield = new YieldColumn($3, $6);
        yield->setFunction($1);
        $$ = yield;
    }
    | agg_function L_PAREN MUL R_PAREN {
        auto expr = new PrimaryExpression(std::string("*"));
        auto yield = new YieldColumn(expr);
        yield->setFunction($1);
        $$ = yield;
    }
    | agg_function L_PAREN MUL R_PAREN KW_AS name_label {
        auto expr = new PrimaryExpression(std::string("*"));
        auto yield = new YieldColumn(expr, $6);
        yield->setFunction($1);
        $$ = yield;
    }
    ;

group_clause
    : yield_columns { $$ = new GroupClause($1); }
    ;

yield_sentence
    : KW_YIELD yield_columns where_clause {
        auto *s = new YieldSentence($2);
        s->setWhereClause($3);
        $$ = s;
    }
    | KW_YIELD KW_DISTINCT yield_columns where_clause {
        auto *s = new YieldSentence($3, true);
        s->setWhereClause($4);
        $$ = s;
    }
    ;

match_sentence
    : KW_MATCH { $$ = new MatchSentence; }
    ;

lookup_sentence
    : KW_LOOKUP KW_ON name_label where_clause yield_clause {
        auto sentence = new LookupSentence($3);
        sentence->setWhereClause($4);
        sentence->setYieldClause($5);
        $$ = sentence;
    }
    ;

order_factor
    : input_ref_expression {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_ASC {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_DESC {
        $$ = new OrderFactor($1, OrderFactor::DESCEND);
    }
    | LABEL {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_ASC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_DESC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::DESCEND);
    }
    ;

order_factors
    : order_factor {
        auto factors = new OrderFactors();
        factors->addFactor($1);
        $$ = factors;
    }
    | order_factors COMMA order_factor {
        $1->addFactor($3);
        $$ = $1;
    }
    ;

order_by_sentence
    : KW_ORDER KW_BY order_factors {
        $$ = new OrderBySentence($3);
    }
    ;

fetch_vertices_sentence
    : KW_FETCH KW_PROP KW_ON fetch_labels vid_list yield_clause {
        $$ = new FetchVerticesSentence($4, $5, $6);
    }
    | KW_FETCH KW_PROP KW_ON fetch_labels vid_ref_expression yield_clause {
        $$ = new FetchVerticesSentence($4, $5, $6);
    }
    | KW_FETCH KW_PROP KW_ON MUL vid_list yield_clause {
        auto labels = new nebula::FetchLabels();
        labels->addLabel(new std::string("*"));
        $$ = new FetchVerticesSentence(labels, $5, $6);
    }
    | KW_FETCH KW_PROP KW_ON MUL vid_ref_expression yield_clause {
        auto labels = new nebula::FetchLabels();
        labels->addLabel(new std::string("*"));
        $$ = new FetchVerticesSentence(labels, $5, $6);
    }
    ;

fetch_labels
    : name_label {
        $$ = new FetchLabels();
        $$->addLabel($1);
    }
    | fetch_labels COMMA name_label {
        $$ = $1;
        $$->addLabel($3);
    }
    ;

edge_key
    : vid R_ARROW vid AT rank {
        $$ = new EdgeKey($1, $3, $5);
    }
    | vid R_ARROW vid {
        $$ = new EdgeKey($1, $3, 0);
    }
    ;

edge_keys
    : edge_key {
        auto edgeKeys = new EdgeKeys();
        edgeKeys->addEdgeKey($1);
        $$ = edgeKeys;
    }
    | edge_keys COMMA edge_key {
        $1->addEdgeKey($3);
        $$ = $1;
    }
    ;

edge_key_ref:
    input_ref_expression R_ARROW input_ref_expression AT input_ref_expression {
        $$ = new EdgeKeyRef($1, $3, $5);
    }
    |
    var_ref_expression R_ARROW var_ref_expression AT var_ref_expression {
        $$ = new EdgeKeyRef($1, $3, $5, false);
    }
    |
    input_ref_expression R_ARROW input_ref_expression {
        $$ = new EdgeKeyRef($1, $3, nullptr);
    }
    |
    var_ref_expression R_ARROW var_ref_expression {
        $$ = new EdgeKeyRef($1, $3, nullptr, false);
    }
    ;

fetch_edges_sentence
    : KW_FETCH KW_PROP KW_ON fetch_labels edge_keys yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    | KW_FETCH KW_PROP KW_ON fetch_labels edge_key_ref yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    ;

fetch_sentence
    : fetch_vertices_sentence { $$ = $1; }
    | fetch_edges_sentence { $$ = $1; }
    ;

scan_part_clause
    : KW_PART expression { $$ = $2; }
    ;
 
scan_from_clause
    : %empty { $$ = nullptr; }
    | KW_FROM expression { $$ = $2; }
    ;
 
scan_limit_clause
    : %empty { $$ = nullptr; }
    | KW_LIMIT expression { $$ = $2; }
    ;
 
scan_sentence
    : KW_SCAN KW_VERTEX name_label scan_part_clause scan_from_clause scan_limit_clause {
        $$ = new ScanSentence($3, $4, $5, $6);
    }
    ;
 

find_path_sentence
    : KW_FIND KW_ALL KW_PATH from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(false, false);
        s->setFrom($4);
        s->setTo($5);
        s->setOver($6);
        s->setStep($7);
        /* s->setWhere($8); */
        $$ = s;
    }
    | KW_FIND KW_SHORTEST KW_PATH from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(true, true);
        s->setFrom($4);
        s->setTo($5);
        s->setOver($6);
        s->setStep($7);
        /* s->setWhere($8); */
        $$ = s;
    }
    | KW_FIND KW_NOLOOP KW_PATH from_clause to_clause over_clause find_path_upto_clause
        /* where_clause */ {
            auto *s = new FindPathSentence(false, true);
            s->setFrom($4);
            s->setTo($5);
            s->setOver($6);
            s->setStep($7);
            /* s->setWhere($8); */
            $$ = s;
        }
    ;

find_path_upto_clause
    : %empty { $$ = new StepClause(5); }
    | KW_UPTO INTEGER KW_STEPS {
        ifOutOfRange($2, @2);
        $$ = new StepClause($2);
    }
    ;

to_clause
    : KW_TO vid_list {
        $$ = new ToClause($2);
    }
    | KW_TO vid_ref_expression {
        $$ = new ToClause($2);
    }
    ;

limit_sentence
    : KW_LIMIT INTEGER {
        ifOutOfRange($2, @2);
        $$ = new LimitSentence(0, $2);
    }
    | KW_LIMIT INTEGER COMMA INTEGER {
        ifOutOfRange($2, @2);
        ifOutOfRange($4, @2);
        $$ = new LimitSentence($2, $4);
    }
    | KW_LIMIT INTEGER KW_OFFSET INTEGER {
        ifOutOfRange($2, @2);
        ifOutOfRange($4, @4);
        $$ = new LimitSentence($4, $2);
    }
    ;

group_by_sentence
    : KW_GROUP KW_BY group_clause yield_clause {
        auto group = new GroupBySentence();
        group->setGroupClause($3);
        group->setYieldClause($4);
        $$ = group;
    }
    ;

use_sentence
    : KW_USE name_label { $$ = new UseSentence($2); }
    ;

opt_if_not_exists
    : %empty { $$=false; }
    | KW_IF KW_NOT KW_EXISTS { $$=true; }
    ;

opt_if_exists
    : %empty { $$=false; }
    | KW_IF KW_EXISTS { $$=true; }
    ;

opt_create_schema_prop_list
    : %empty {
        $$ = nullptr;
    }
    | create_schema_prop_list {
        $$ = $1;
    }
    ;

create_schema_prop_list
    : create_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | create_schema_prop_list COMMA create_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

create_schema_prop_item
    : KW_TTL_DURATION ASSIGN INTEGER {
        ifOutOfRange($3, @3);
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN STRING {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_tag_sentence
    : KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, new ColumnSpecificationList(), $7, $3);
    }
    | KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, $6, $8, $3);
    }
    | KW_CREATE KW_TAG opt_if_not_exists name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($9 == nullptr) {
            $9 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($4, $6, $9, $3);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG name_label alter_schema_opt_list {
        $$ = new AlterTagSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_TAG name_label alter_schema_prop_list {
        $$ = new AlterTagSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_TAG name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterTagSentence($3, $4, $5);
    }
    ;

alter_schema_opt_list
    : alter_schema_opt_item {
        $$ = new AlterSchemaOptList();
        $$->addOpt($1);
    }
    | alter_schema_opt_list COMMA alter_schema_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_opt_item
    : KW_ADD L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::ADD, $3);
    }
    | KW_CHANGE L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::CHANGE, $3);
    }
    | KW_DROP L_PAREN column_name_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::DROP, $3);
    }
    ;

alter_schema_prop_list
    : alter_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | alter_schema_prop_list COMMA alter_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_prop_item
    : KW_TTL_DURATION ASSIGN INTEGER {
        ifOutOfRange($3, @3);
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN STRING {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_edge_sentence
    : KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4,  new ColumnSpecificationList(), $7, $3);
    }
    | KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4, $6, $8, $3);
    }
    | KW_CREATE KW_EDGE opt_if_not_exists name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($9 == nullptr) {
            $9 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($4, $6, $9, $3);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE name_label alter_schema_opt_list {
        $$ = new AlterEdgeSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_EDGE name_label alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_EDGE name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, $4, $5);
    }
    ;

column_name_list
    : name_label {
        $$ = new ColumnNameList();
        $$->addColumn($1);
    }
    | column_name_list COMMA name_label {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec_list
    : column_spec {
        $$ = new ColumnSpecificationList();
        $$->addColumn($1);
    }
    | column_spec_list COMMA column_spec {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec
    : name_label type_spec { $$ = new ColumnSpecification($2, $1); }
    | name_label type_spec KW_DEFAULT expression {
        $$ = new ColumnSpecification($2, $1);
        $$->setValue($4);
    }
    ;

describe_tag_sentence
    : KW_DESCRIBE KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    | KW_DESC KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    ;

describe_edge_sentence
    : KW_DESCRIBE KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    | KW_DESC KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    ;

drop_tag_sentence
    : KW_DROP KW_TAG opt_if_exists name_label {
        $$ = new DropTagSentence($4, $3);
    }
    ;

drop_edge_sentence
    : KW_DROP KW_EDGE opt_if_exists name_label {
        $$ = new DropEdgeSentence($4, $3);
    }
    ;

create_tag_index_sentence
    : KW_CREATE KW_TAG KW_INDEX opt_if_not_exists name_label KW_ON name_label L_PAREN column_name_list R_PAREN {
        $$ = new CreateTagIndexSentence($5, $7, $9, $4);
    }
    ;

create_edge_index_sentence
    : KW_CREATE KW_EDGE KW_INDEX opt_if_not_exists name_label KW_ON name_label L_PAREN column_name_list R_PAREN {
        $$ = new CreateEdgeIndexSentence($5, $7, $9, $4);
    }
    ;

drop_tag_index_sentence
    : KW_DROP KW_TAG KW_INDEX opt_if_exists name_label {
        $$ = new DropTagIndexSentence($5, $4);
    }
    ;

drop_edge_index_sentence
    : KW_DROP KW_EDGE KW_INDEX opt_if_exists name_label {
        $$ = new DropEdgeIndexSentence($5, $4);
    }
    ;

describe_tag_index_sentence
    : KW_DESCRIBE KW_TAG KW_INDEX name_label {
        $$ = new DescribeTagIndexSentence($4);
    }
    | KW_DESC KW_TAG KW_INDEX name_label {
        $$ = new DescribeTagIndexSentence($4);
    }
    ;

describe_edge_index_sentence
    : KW_DESCRIBE KW_EDGE KW_INDEX name_label {
        $$ = new DescribeEdgeIndexSentence($4);
    }
    | KW_DESC KW_EDGE KW_INDEX name_label {
        $$ = new DescribeEdgeIndexSentence($4);
    }
    ;

rebuild_tag_index_sentence
    : KW_REBUILD KW_TAG KW_INDEX name_label {
        $$ = new RebuildTagIndexSentence($4, false);
    }
    | KW_REBUILD KW_TAG KW_INDEX name_label KW_OFFLINE {
        $$ = new RebuildTagIndexSentence($4, true);
    }
    ;

rebuild_edge_index_sentence
    : KW_REBUILD KW_EDGE KW_INDEX name_label {
        $$ = new RebuildEdgeIndexSentence($4, false);
    }
    | KW_REBUILD KW_EDGE KW_INDEX name_label KW_OFFLINE {
        $$ = new RebuildEdgeIndexSentence($4, true);
    }
    ;

rebuild_sample_sentence
    : KW_REBUILD KW_SAMPLE KW_ALL {
        $$ = new RebuildSampleSentence(false);
    }
    | KW_REBUILD KW_SAMPLE KW_ALL KW_FORCE {
        $$ = new RebuildSampleSentence(true);
    }
    | KW_REBUILD KW_SAMPLE KW_ALL KW_TAG {
        $$ = new RebuildSampleSentence(nullptr, true, false);
    }
    | KW_REBUILD KW_SAMPLE KW_ALL KW_TAG KW_FORCE {
        $$ = new RebuildSampleSentence(nullptr, true, true);
    }
    | KW_REBUILD KW_SAMPLE KW_ALL KW_EDGE {
        $$ = new RebuildSampleSentence(nullptr, false, false);
    }
    | KW_REBUILD KW_SAMPLE KW_ALL KW_EDGE KW_FORCE {
        $$ = new RebuildSampleSentence(nullptr, false, true);
    }
    | KW_REBUILD KW_SAMPLE KW_TAG sample_labels {
        $$ = new RebuildSampleSentence($4, true, false);
    }
    | KW_REBUILD KW_SAMPLE KW_TAG sample_labels KW_FORCE {
        $$ = new RebuildSampleSentence($4, true, true);
    }
    | KW_REBUILD KW_SAMPLE KW_EDGE sample_labels {
        $$ = new RebuildSampleSentence($4, false, false);
    }
    | KW_REBUILD KW_SAMPLE KW_EDGE sample_labels KW_FORCE {
        $$ = new RebuildSampleSentence($4, false, true);
    }
    ;

traverse_sentence
    : L_PAREN set_sentence R_PAREN { $$ = $2; }
    | go_sentence { $$ = $1; }
    | match_sentence { $$ = $1; }
    | lookup_sentence { $$ = $1; }
    | scan_sentence { $$ = $1; }
    | group_by_sentence { $$ = $1; }
    | order_by_sentence { $$ = $1; }
    | fetch_sentence { $$ = $1; }
    | find_path_sentence { $$ = $1; }
    | limit_sentence { $$ = $1; }
    | yield_sentence { $$ = $1; }
    | sample_nb_sentence { $$ = $1; }
    | sample_sentence { $$ =$1; }
    | random_walk_sentence { $$ = $1; }
    ;

piped_sentence
    : traverse_sentence { $$ = $1; }
    | piped_sentence PIPE traverse_sentence { $$ = new PipedSentence($1, $3); }
    ;

set_sentence
    : piped_sentence { $$ = $1; }
    | set_sentence KW_UNION KW_ALL piped_sentence { $$ = new SetSentence($1, SetSentence::UNION, $4); }
    | set_sentence KW_UNION piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $3);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_UNION KW_DISTINCT piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $4);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_INTERSECT piped_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS piped_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    ;

assignment_sentence
    : VARIABLE ASSIGN set_sentence {
        $$ = new AssignmentSentence($1, $3);
    }
    ;

insert_vertex_sentence
    : KW_INSERT KW_VERTEX vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($3, $5);
    }
    | KW_INSERT KW_VERTEX KW_NO KW_OVERWRITE vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($5, $7, false /* not overwritable */);
    }
    ;

vertex_tag_list
    : vertex_tag_item {
        $$ = new VertexTagList();
        $$->addTagItem($1);
    }
    | vertex_tag_list COMMA vertex_tag_item {
        $$ = $1;
        $$->addTagItem($3);
    }
    ;

vertex_tag_item
    : name_label L_PAREN R_PAREN{
        $$ = new VertexTagItem($1);
    }
    | name_label L_PAREN prop_list R_PAREN {
        $$ = new VertexTagItem($1, $3);
    }
    ;

prop_list
    : name_label {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA name_label {
        $$ = $1;
        $$->addProp($3);
    }
    | prop_list COMMA {
        $$ = $1;
    }
    ;

vertex_row_list
    : vertex_row_item {
        $$ = new VertexRowList();
        $$->addRow($1);
    }
    | vertex_row_list COMMA vertex_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

vertex_row_item
    : vid COLON L_PAREN R_PAREN {
        $$ = new VertexRowItem($1, new ValueList());
    }
    | vid COLON L_PAREN value_list R_PAREN {
        $$ = new VertexRowItem($1, $4);
    }
    ;

value_list
    : expression {
        $$ = new ValueList();
        $$->addValue($1);
    }
    | value_list COMMA expression {
        $$ = $1;
        $$->addValue($3);
    }
    | value_list COMMA {
        $$ = $1;
    }
    ;

insert_edge_sentence
    : KW_INSERT KW_EDGE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps(new PropertyList());
        sentence->setRows($7);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setRows($8);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps(new PropertyList());
        sentence->setRows($9);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setRows($10);
        $$ = sentence;
    }
    ;

edge_row_list
    : edge_row_item {
        $$ = new EdgeRowList();
        $$->addRow($1);
    }
    | edge_row_list COMMA edge_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

edge_row_item
    : vid R_ARROW vid COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, new ValueList());
    }
    | vid R_ARROW vid COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $6);
    }
    | vid R_ARROW vid AT rank COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, new ValueList());
    }
    | vid R_ARROW vid AT rank COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, $8);
    }
    ;

rank: unary_integer { $$ = $1; };

update_vertex_sentence
    : KW_UPDATE KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhenClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    | KW_UPSERT KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setInsertable(true);
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhenClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    ;

update_list
    : update_item {
        $$ = new UpdateList();
        $$->addItem($1);
    }
    | update_list COMMA update_item {
        $$ = $1;
        $$->addItem($3);
    }
    ;

update_item
    : name_label ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    | alias_ref_expression ASSIGN expression {
        $$ = new UpdateItem($1, $3);
        delete $1;
    }
    ;

update_edge_sentence
    : KW_UPDATE KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setEdgeType($7);
        sentence->setUpdateList($9);
        sentence->setWhenClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setEdgeType($7);
        sentence->setUpdateList($9);
        sentence->setWhenClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setEdgeType($9);
        sentence->setUpdateList($11);
        sentence->setWhenClause($12);
        sentence->setYieldClause($13);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setEdgeType($9);
        sentence->setUpdateList($11);
        sentence->setWhenClause($12);
        sentence->setYieldClause($13);
        $$ = sentence;
    }
    ;

delete_vertex_sentence
    : KW_DELETE KW_VERTEX vid_list {
        auto sentence = new DeleteVerticesSentence($3);
        $$ = sentence;
    }
    ;

download_sentence
    : KW_DOWNLOAD KW_HDFS STRING {
        auto sentence = new DownloadSentence();
        sentence->setUrl($3);
        $$ = sentence;
    }
    | KW_DOWNLOAD KW_EDGE name_label KW_HDFS STRING {
        auto sentence = new DownloadSentence();
        sentence->setEdge($3);
        sentence->setUrl($5);
        $$ = sentence;
    }
    | KW_DOWNLOAD KW_TAG name_label KW_HDFS STRING {
        auto sentence = new DownloadSentence();
        sentence->setTag($3);
        sentence->setUrl($5);
        $$ = sentence;
    }
    ;

delete_edge_sentence
    : KW_DELETE KW_EDGE name_label edge_keys {
        auto sentence = new DeleteEdgesSentence($3, $4);
        $$ = sentence;
    }
    ;

ingest_sentence
    : KW_INGEST {
        auto sentence = new IngestSentence();
        $$ = sentence;
    }
    | KW_INGEST KW_EDGE name_label {
        auto sentence = new IngestSentence();
        sentence->setEdge($3);
        $$ = sentence;
    }
    | KW_INGEST KW_TAG name_label {
        auto sentence = new IngestSentence();
        sentence->setTag($3);
        $$ = sentence;
    }
    ;

admin_sentence
    : KW_SUBMIT KW_JOB admin_operation {
        auto sentence = new AdminSentence("add_job");
        sentence->addPara(*$3);
        delete $3;
        $$ = sentence;
    }
    | KW_SHOW KW_JOBS {
        auto sentence = new AdminSentence("show_jobs");
        $$ = sentence;
    }
    | KW_SHOW KW_JOB INTEGER {
        auto sentence = new AdminSentence("show_job");
        sentence->addPara(std::to_string($3));
        $$ = sentence;
    }
    | KW_STOP KW_JOB INTEGER {
        auto sentence = new AdminSentence("stop_job");
        sentence->addPara(std::to_string($3));
        $$ = sentence;
    }
    | KW_RECOVER KW_JOB {
        auto sentence = new AdminSentence("recover_job");
        $$ = sentence;
    }
    ;

admin_operation
    : KW_COMPACT { $$ = new std::string("compact"); }
    | KW_FLUSH   { $$ = new std::string("flush"); }
    | admin_operation admin_para {
        $$ = new std::string(*$1 + " " + *$2);
    }
    ;

admin_para
    : name_label ASSIGN name_label {
        auto left = *$1;
        auto right = *$3;
        $$ = new std::string(*$1 + "=" + *$3);
    }
    ;

show_sentence
    : KW_SHOW KW_HOSTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowHosts);
    }
    | KW_SHOW KW_SPACES {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSpaces);
    }
    | KW_SHOW KW_PARTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowParts);
    }
    | KW_SHOW KW_PART integer_list {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowParts, $3);
    }
    | KW_SHOW KW_PARTS integer_list {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowParts, $3);
    }
    | KW_SHOW KW_TAGS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowTags);
    }
    | KW_SHOW KW_EDGES {
         $$ = new ShowSentence(ShowSentence::ShowType::kShowEdges);
    }
    | KW_SHOW KW_TAG KW_INDEXES {
         $$ = new ShowSentence(ShowSentence::ShowType::kShowTagIndexes);
    }
    | KW_SHOW KW_EDGE KW_INDEXES {
         $$ = new ShowSentence(ShowSentence::ShowType::kShowEdgeIndexes);
    }
    | KW_SHOW KW_USERS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowUsers);
    }
    | KW_SHOW KW_ROLES KW_IN name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowRoles, $4);
    }
    | KW_SHOW KW_CONFIGS show_config_item {
        $$ = new ConfigSentence(ConfigSentence::SubType::kShow, $3);
    }
    | KW_SHOW KW_CREATE KW_SPACE name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateSpace, $4);
    }
    | KW_SHOW KW_CREATE KW_TAG name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateTag, $4);
    }
    | KW_SHOW KW_CREATE KW_TAG KW_INDEX name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateTagIndex, $5);
    }
    | KW_SHOW KW_CREATE KW_EDGE name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateEdge, $4);
    }
    | KW_SHOW KW_CREATE KW_EDGE KW_INDEX name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateEdgeIndex, $5);
    }
    | KW_SHOW KW_TAG KW_INDEX KW_STATUS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowTagIndexStatus);
    }
    | KW_SHOW KW_EDGE KW_INDEX KW_STATUS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowEdgeIndexStatus);
    }
    | KW_SHOW KW_SNAPSHOTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSnapshots);
    }
    | KW_SHOW KW_CHARSET {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCharset);
    }
    | KW_SHOW KW_COLLATION {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCollation);
    }
    | KW_SHOW KW_SAMPLE KW_STATUS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSampleStatus);
    }
    | KW_SHOW KW_SAMPLE KW_STATUS KW_STATUS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSampleStatus, new std::string("d"));
    }
    ;

config_module_enum
    : KW_GRAPH      { $$ = ConfigModule::GRAPH; }
    | KW_META       { $$ = ConfigModule::META; }
    | KW_STORAGE    { $$ = ConfigModule::STORAGE; }
    ;

get_config_item
    : config_module_enum COLON name_label {
        $$ = new ConfigRowItem($1, $3);
    }
    | name_label {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1);
    }
    ;

set_config_item
    : config_module_enum COLON name_label ASSIGN expression {
        $$ = new ConfigRowItem($1, $3, $5);
    }
    | name_label ASSIGN expression {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1, $3);
    }
    | config_module_enum COLON name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem($1, $3, $6);
    }
    | name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1, $4);
    }
    ;

show_config_item
    : %empty {
        $$ = nullptr;
    }
    | config_module_enum {
        $$ = new ConfigRowItem($1);
    }
    ;

create_space_sentence
    : KW_CREATE KW_SPACE opt_if_not_exists name_label {
        auto sentence = new CreateSpaceSentence($4, $3);
        $$ = sentence;
    }
    | KW_CREATE KW_SPACE opt_if_not_exists name_label L_PAREN space_opt_list R_PAREN {
        auto sentence = new CreateSpaceSentence($4, $3);
        sentence->setOpts($6);
        $$ = sentence;
    }
    ;

describe_space_sentence
    : KW_DESCRIBE KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    | KW_DESC KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    ;

space_opt_list
    : space_opt_item {
        $$ = new SpaceOptList();
        $$->addOpt($1);
    }
    | space_opt_list COMMA space_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    | space_opt_list COMMA {
        $$ = $1;
    }
    ;

space_opt_item
    : KW_PARTITION_NUM ASSIGN INTEGER {
        ifOutOfRange($3, @3);
        $$ = new SpaceOptItem(SpaceOptItem::PARTITION_NUM, $3);
    }
    | KW_REPLICA_FACTOR ASSIGN INTEGER {
        ifOutOfRange($3, @3);
        $$ = new SpaceOptItem(SpaceOptItem::REPLICA_FACTOR, $3);
    }
    | KW_CHARSET ASSIGN name_label {
        // Currently support utf8, it is an alias for utf8mb4
        $$ = new SpaceOptItem(SpaceOptItem::CHARSET, *$3);
        delete $3;
    }
    | KW_COLLATE ASSIGN name_label {
        // Currently support utf8_bin, it is an alias for utf8mb4_bin
        $$ = new SpaceOptItem(SpaceOptItem::COLLATE, *$3);
        delete $3;
    }
    // TODO(YT) Create Spaces for different engines
    // KW_ENGINE_TYPE ASSIGN name_label
    ;

drop_space_sentence
    : KW_DROP KW_SPACE opt_if_exists name_label {
        $$ = new DropSpaceSentence($4, $3);
    }
    ;

//  User manager sentences.

create_user_sentence
    : KW_CREATE KW_USER opt_if_not_exists name_label KW_WITH KW_PASSWORD STRING {
        $$ = new CreateUserSentence($4, $7, $3);
    }
    | KW_CREATE KW_USER opt_if_not_exists name_label {
        $$ = new CreateUserSentence($4, new std::string(""), $3);
    }
    ;

alter_user_sentence
    : KW_ALTER KW_USER name_label KW_WITH KW_PASSWORD STRING {
        $$ = new AlterUserSentence($3, $6);
    }
    ;

drop_user_sentence
    : KW_DROP KW_USER opt_if_exists name_label {
        auto sentence = new DropUserSentence($4, $3);
        $$ = sentence;
    }
    ;

change_password_sentence
    : KW_CHANGE KW_PASSWORD name_label KW_FROM STRING KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5, $7);
        $$ = sentence;
    }
    ;

role_type_clause
    : KW_GOD { $$ = new RoleTypeClause(RoleTypeClause::GOD); }
    | KW_ADMIN { $$ = new RoleTypeClause(RoleTypeClause::ADMIN); }
    | KW_DBA { $$ = new RoleTypeClause(RoleTypeClause::DBA); }
    | KW_USER { $$ = new RoleTypeClause(RoleTypeClause::USER); }
    | KW_GUEST { $$ = new RoleTypeClause(RoleTypeClause::GUEST); }
    ;

acl_item_clause
    : KW_ROLE role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(true, $4);
        sentence->setRoleTypeClause($2);
        $$ = sentence;
    }
    | role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(false, $3);
        sentence->setRoleTypeClause($1);
        $$ = sentence;
    }
    ;

grant_sentence
    : KW_GRANT acl_item_clause KW_TO name_label {
        auto sentence = new GrantSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

revoke_sentence
    : KW_REVOKE acl_item_clause KW_FROM name_label {
        auto sentence = new RevokeSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

get_config_sentence
    : KW_GET KW_CONFIGS get_config_item {
        $$ = new ConfigSentence(ConfigSentence::SubType::kGet, $3);
    }
    ;

set_config_sentence
    : KW_UPDATE KW_CONFIGS set_config_item {
        $$ = new ConfigSentence(ConfigSentence::SubType::kSet, $3);
    }
    | KW_UPDATE KW_CONFIGS set_config_item KW_FORCE {
        $$ = new ConfigSentence(ConfigSentence::SubType::kSet, $3, true);
    }
    ;

host_list
    : host_item {
        $$ = new HostList();
        $$->addHost($1);
    }
    | host_list COMMA host_item {
        $$ = $1;
        $$->addHost($3);
    }
    | host_list COMMA {
        $$ = $1;
    }
    ;

host_item
    : IPV4 COLON port {
        $$ = new nebula::HostAddr();
        $$->first = $1;
        $$->second = $3;
    }

port : INTEGER { $$ = $1; }

integer_list
    : INTEGER {
        $$ = new std::vector<int32_t>();
        $$->emplace_back($1);
    }
    | integer_list COMMA INTEGER {
        $$ = $1;
        $$->emplace_back($3);
    }
    | integer_list COMMA {
        $$ = $1;
    }
    ;

balance_sentence
    : KW_BALANCE KW_LEADER {
        $$ = new BalanceSentence(BalanceSentence::SubType::kLeader);
    }
    | KW_BALANCE KW_DATA {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData);
    }
    | KW_BALANCE KW_DATA INTEGER {
        ifOutOfRange($3, @3);
        $$ = new BalanceSentence($3);
    }
    | KW_BALANCE KW_DATA KW_STOP {
        $$ = new BalanceSentence(BalanceSentence::SubType::kDataStop);
    }
    | KW_BALANCE KW_DATA KW_REMOVE host_list {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData, $4);
    }
    ;

create_snapshot_sentence
    : KW_CREATE KW_SNAPSHOT {
        $$ = new CreateSnapshotSentence();
    }
    ;

drop_snapshot_sentence
    : KW_DROP KW_SNAPSHOT name_label {
        $$ = new DropSnapshotSentence($3);
    }
    ;

mutate_sentence
    : insert_vertex_sentence { $$ = $1; }
    | insert_edge_sentence { $$ = $1; }
    | update_vertex_sentence { $$ = $1; }
    | update_edge_sentence { $$ = $1; }
    | delete_vertex_sentence { $$ = $1; }
    | delete_edge_sentence { $$ = $1; }
    | download_sentence { $$ = $1; }
    | ingest_sentence { $$ = $1; }
    | admin_sentence { $$ = $1; }
    ;

maintain_sentence
    : create_space_sentence { $$ = $1; }
    | describe_space_sentence { $$ = $1; }
    | drop_space_sentence { $$ = $1; }
    | create_tag_sentence { $$ = $1; }
    | create_edge_sentence { $$ = $1; }
    | alter_tag_sentence { $$ = $1; }
    | alter_edge_sentence { $$ = $1; }
    | describe_tag_sentence { $$ = $1; }
    | describe_edge_sentence { $$ = $1; }
    | drop_tag_sentence { $$ = $1; }
    | drop_edge_sentence { $$ = $1; }
    | create_tag_index_sentence { $$ = $1; }
    | create_edge_index_sentence { $$ = $1; }
    | drop_tag_index_sentence { $$ = $1; }
    | drop_edge_index_sentence { $$ = $1; }
    | describe_tag_index_sentence { $$ = $1; }
    | describe_edge_index_sentence { $$ = $1; }
    | rebuild_tag_index_sentence { $$ = $1; }
    | rebuild_edge_index_sentence { $$ = $1; }
    | rebuild_sample_sentence { $$ = $1; }
    | show_sentence { $$ = $1; }
    ;
    | create_user_sentence { $$ = $1; }
    | alter_user_sentence { $$ = $1; }
    | drop_user_sentence { $$ = $1; }
    | change_password_sentence { $$ = $1; }
    | grant_sentence { $$ = $1; }
    | revoke_sentence { $$ = $1; }
    | get_config_sentence { $$ = $1; }
    | set_config_sentence { $$ = $1; }
    | balance_sentence { $$ = $1; }
    | create_snapshot_sentence { $$ = $1; };
    | drop_snapshot_sentence { $$ = $1; };
    ;

return_sentence
    : KW_RETURN VARIABLE KW_IF VARIABLE KW_IS KW_NOT KW_NULL {
        $$ = new ReturnSentence($2, $4);
    }

process_control_sentence
    : return_sentence { $$ = $1; }
    ;

sentence
    : maintain_sentence { $$ = $1; }
    | use_sentence { $$ = $1; }
    | set_sentence { $$ = $1; }
    | assignment_sentence { $$ = $1; }
    | mutate_sentence { $$ = $1; }
    | process_control_sentence { $$ = $1; }
    ;

sentences
    : sentence {
        $$ = new SequentialSentences($1);
        *sentences = $$;
    }
    | sentences SEMICOLON sentence {
        if ($1 == nullptr) {
            $$ = new SequentialSentences($3);
            *sentences = $$;
        } else {
            $$ = $1;
            $1->addSentence($3);
        }
    }
    | sentences SEMICOLON {
        $$ = $1;
    }
    | %empty {
        $$ = nullptr;
    }
    ;


%%

#include "GraphScanner.h"
void nebula::GraphParser::error(const nebula::GraphParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    if (msg.empty()) {
        os << "syntax error";
    } else {
        os << msg;
    }

    auto *query = scanner.query();
    if (query == nullptr) {
        os << " at " << loc;
        errmsg = os.str();
        return;
    }

    auto begin = loc.begin.column > 0 ? loc.begin.column - 1 : 0;
    if ((loc.end.filename
        && (!loc.begin.filename
            || *loc.begin.filename != *loc.end.filename))
        || loc.begin.line < loc.end.line
        || begin >= query->size()) {
        os << " at " << loc;
    } else if (loc.begin.column < (loc.end.column ? loc.end.column - 1 : 0)) {
        uint32_t len = loc.end.column - loc.begin.column;
        if (len > 80) {
            len = 80;
        }
        os << " near `" << query->substr(begin, len) << "'";
    } else {
        os << " near `" << query->substr(begin, 8) << "'";
    }

    errmsg = os.str();
}

// check the positive integer boundary
// parameter input accept the INTEGER value
// which filled as uint64_t
// so the conversion is expected
void ifOutOfRange(const int64_t input,
                  const nebula::GraphParser::location_type& loc) {
    if ((uint64_t)input >= MAX_ABS_INTEGER) {
        throw nebula::GraphParser::syntax_error(loc, "Out of range:");
    }
}

static int yylex(nebula::GraphParser::semantic_type* yylval,
                 nebula::GraphParser::location_type *yylloc,
                 nebula::GraphScanner& scanner) {
    return scanner.yylex(yylval, yylloc);
}

