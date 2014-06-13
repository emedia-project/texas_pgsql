-module(texas_pgsql).

-export([start/0]).
-export([connect/6, exec/2, close/1]).
-export([create_table/2]).
-export([insert/3, select/4, update/4, delete/3]).

-define(STRING_SEPARATOR, $').
-define(STRING_QUOTE, $').

-type connection() :: any().
-type err() :: any().
-type tablename() :: atom().
-type data() :: any().
-type clause_type() :: where | group | order | limit.
-type clause() :: {clause_type(), string(), [tuple()]} |
                  {clause_type(), string(), []}.
-type clauses() :: [clause()] | [].

-spec start() -> ok.
start() ->
  ok.

-spec connect(string(), string(), string(), integer(), string(), any()) -> 
  {ok, connection()} | {error, err()}.
connect(User, Password, Server, Port, Database, _Options) ->
  lager:info("Open database pgsql://~p:~p@~p:~p/~p", [User, Password, Server, Port, Database]),
  pgsql:connect(Server, User, Password, [{database, Database}, {port, list_to_integer(Port)}]).

-spec close(connection()) -> ok | error.
close(Conn) ->
  pgsql:close(Conn).

-spec create_table(connection(), tablename()) -> ok | error.
create_table(Conn, Table) ->
  SQLCmd = sql(
    create_table, 
    atom_to_list(Table), 
    lists:map(fun(Field) ->
            sql(
              column_def, 
              atom_to_list(Field),
              Table:type(Field),
              Table:len(Field),
              Table:autoincrement(Field),
              Table:not_null(Field),
              Table:unique(Field),
              Table:default(Field))
        end, Table:fields())),
  lager:info("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok, _, _} -> ok;
    _ -> error
  end.

-spec insert(connection(), tablename(), data()) -> data() | {error, err()}.
insert(Conn, Table, Record) ->
  {Fields, Values} = lists:foldl(fun(Field, {FieldsAcc, ValuesAcc}) ->
          case Record:Field() of
            undefined -> {FieldsAcc, ValuesAcc};
            Value -> {FieldsAcc ++ [atom_to_list(Field)], 
                      ValuesAcc ++ [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]}
          end
      end, {[], []}, Table:fields()),
  SQLCmd = sql(insert, atom_to_list(Table), Fields, Values, Table:table_pk_id()),
  lager:info("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok, 1, [{column, Col, _, _, _, _}],[{ID}]} -> 
      ACol = binary_to_atom(Col, utf8), 
      select(Conn, Table, first, [{where, io_lib:format("~p = :id", [ACol]), [{id, texas_type:to(Table:type(ACol), ID)}]}]);
    {error, Error} -> {error, Error};
    _ -> Record
  end.

-spec select(connection(), tablename(), first | all, clauses()) -> 
  data() | [data()] | [] | {error, err()}.
select(Conn, Table, Type, Clauses) -> 
  SQLCmd = sql(select, atom_to_list(Table), sql(clause, Clauses), Type),
  lager:info("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok, _, []} -> [];
    {ok, Cols, Datas} -> 
      ColsList = lists:map(fun({column, Col, _, _, _, _}) -> binary_to_atom(Col, utf8) end, Cols),
      case Type of
        first -> 
          [Data|_] = Datas, 
          Table:new(assoc(Table, ColsList, Data));
        _ -> 
          lists:map(fun(Data) ->
                Table:new(assoc(Table, ColsList, Data))
            end, Datas)
      end;
    E -> E
  end.

-spec update(connection(), tablename(), data(), [tuple()]) -> [data()] | {error, err()}.
update(Conn, Table, Record, UpdateData) ->
  Where = join(lists:foldl(fun(Field, W) ->
            case Record:Field() of
              undefined -> W;
              Value -> W ++ [{Field, Value}]
            end
        end, [], Table:fields()), " AND "),
  Set = join(UpdateData, ", "),
  SQLCmd = "UPDATE " ++ atom_to_list(Table) ++ " SET " ++ Set ++ " WHERE " ++ Where ++ ";",
  lager:info("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok, _} -> 
      UpdateRecord = lists:foldl(fun({Field, Value}, Rec) ->
              Rec:Field(Value)
          end, Record, UpdateData),
      select(Conn, Table, all, [texas_sql:record_to_where_clause(Table, UpdateRecord)]);
    E -> E
  end.

-spec delete(connection(), tablename(), data()) -> ok | {error, err()}.
delete(Conn, Table, Record) ->
  WhereClause = texas_sql:record_to_where_clause(Table, Record),
  SQLCmd = sql(delete, atom_to_list(Table), sql(clause, [WhereClause])),
  lager:info("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok, _} -> ok;
    E -> E
  end.

% Private --

exec(SQL, Conn) ->
  pgsql:squery(Conn, SQL).

assoc(Table, Cols, Datas) ->
  lists:map(fun({Col, Data}) ->
        case Data of
          null -> {Col, undefined};
          _ -> {Col, texas_type:to(Table:type(Col), Data)}
        end
    end, lists:zip(Cols, tuple_to_list(Datas))).

join(KVList, Sep) ->
  string:join(lists:map(fun({K, V}) ->
          io_lib:format("~p = ~s", [K, texas_sql:to_sql_string(V, ?STRING_SEPARATOR, ?STRING_QUOTE)])
      end, KVList), Sep).

sql(create_table, Name, ColDefs) -> 
  "CREATE TABLE IF NOT EXISTS " ++ Name ++ " (" ++ string:join(ColDefs, ", ") ++ ");";
sql(delete, Name, Clauses) ->
  "DELETE FROM " ++ Name ++ " " ++ string:join(Clauses, " ") ++ ";".
sql(type, id, {ok, true}, _) -> " BIGSERIAL PRIMARY KEY";
sql(type, id, _, _) -> " BIGINT";
sql(type, integer, {ok, true}, _) -> " BIGSERIAL PRIMARY KEY";
sql(type, integer, _, _) -> " BIGINT";
sql(type, string, _, {ok, Len}) -> " VARCHAR(" ++ integer_to_list(Len) ++ ")";
sql(type, string, _, _) -> " TEXT";
sql(type, float, _, _) -> " REAL";
sql(type, _, _, _) -> " TEXT";
sql(select, Name, Clauses, Type) ->
  "SELECT * FROM " ++ Name ++ " " ++ string:join(Clauses, " ") ++ case Type of
    first -> " LIMIT 1;";
    _ -> ";"
  end.
sql(insert, Table, Fields, Values, PkID) ->
  "INSERT INTO " ++ Table ++ "(" ++ string:join(Fields, ", ") ++ ") VALUES (" ++ string:join(Values, ", ") ++ ")" ++ case PkID of
    {ok, ID} -> " RETURNING " ++ atom_to_list(ID) ++ ";";
    _ -> ";"
  end.
sql(column_def, Name, Type, Len, Autoincrement, NotNull, Unique, Default) ->
  Name ++ 
  sql(type, Type, Autoincrement, Len) ++ 
  sql(notnull, NotNull) ++
  sql(unique, Unique) ++
  sql(default, Default).
sql(where, Data) -> "WHERE " ++ Data;
sql(group, Data) -> "GROUP BY " ++ Data;
sql(order, Data) -> "ORDER BY " ++ Data;
sql(limit, Data) -> "LIMIT " ++ Data;
sql(notnull, {ok, true}) -> " NOT NULL";
sql(unique, {ok, true}) -> " UNIQUE";
sql(default, {ok, Value}) -> io_lib:format(" DEFAULT ~s", [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]);
sql(clause, Clauses) when is_list(Clauses) ->
  lists:map(fun(Clause) -> sql(clause, Clause) end, Clauses);
sql(clause, {Type, Str, Params}) ->
  WhereClause = lists:foldl(fun({Field, Value}, Clause) ->
        estring:gsub(Clause, ":" ++ atom_to_list(Field), texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE))
    end, Str, Params),
  sql(Type, WhereClause);
sql(clause, {Type, Str}) ->
  sql(clause, {Type, Str, []});
sql(_, _) -> "".
