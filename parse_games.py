import chess.pgn
import glob
from credentials import credentials
from SQL_functions import create_connection
from SQL_functions import execute_query
from re import search

# connect to the database chess
HOST, USER, PASSWORD, DB_NAME = credentials()
connector = create_connection(HOST, USER, PASSWORD, DB_NAME)

# First drop the tables if they are already there
execute_query(connector, "DROP TABLE IF EXISTS chess")
execute_query(connector, "DROP TABLE IF EXISTS matches")

# The definition of the table
# Note that chess piece notation is case-dependent (i.e. P is a white pawn, p is a black pawn)
create_chess_table = """
CREATE TABLE chess (
  id INT AUTO_INCREMENT,
  match_id TEXT, 
  taken_piece CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin,
  taker_piece CHAR(1)  CHARACTER SET utf8 COLLATE utf8_bin,
  square INT,
  turn INT,
  ply INT,
  en_passant BOOLEAN,
  PRIMARY KEY(id)
) ENGINE = InnoDB
"""

create_match_table = """
CREATE TABLE matches (
    match_id TEXT,
    WhiteElo INT,
    BlackElo INT,
    ECO CHAR(3),
    Opening TEXT
) Engine = InnoDB
"""

execute_query(connector, create_chess_table)
execute_query(connector, create_match_table)

# Now we can populate the table. Iterate through the matches, extracting the relevant data
for pgn_file in glob.iglob("pgn_files/*.pgn"):
    # single iteration version
    # the_match = glob.glob('pgn_files/lichess_dbo_standard_rated_2014-01.pgn')[0]

    pgn = open(pgn_file, 'r')
    game = chess.pgn.read_game(pgn)

    while game is not None:
        vals = ''
        try:
            if '%eval' in game.variations[0].comment:
                # moves have computer evaluation
                node = game.next()
                board = chess.Board()
                game_has_a_capture = False
                match_id = search('[A-Za-z0-9]*$', game.headers.get("Site")).group(0)

                ECO = game.headers.get("ECO")
                Opening = game.headers.get("Opening").replace("'", "''")
                WhiteElo = game.headers.get("WhiteElo")
                BlackElo = game.headers.get("BlackElo")

                # parse the game, building the query, then execute later to save time
                while not node.is_end():  # loop while the game isn't over
                    en_passant = False
                    if board.is_capture(node.move):
                        game_has_a_capture = True
                        # Now we identify the taken piece by looking for the piece that the taking piece landed on
                        if board.piece_at(node.move.to_square) is None:
                            # if a pawn takes another pawn en passant, the taken piece will
                            # be on a different square than the taking piece ends up on
                            s = board.piece_at(node.move.from_square).symbol()
                            # if a white pawn takes en passant, the taken piece can only be a black pawn and v.v
                            taken = s.lower() if s.isupper() else s.upper()
                            en_passant = True
                        else:
                            taken = board.piece_at(node.move.to_square)

                        # now format the next part of the query
                        vals = vals + f""" ('{match_id}', '{taken}', '{board.piece_at(node.move.from_square)}', """ \
                                      f"""{node.move.to_square}, {board.fullmove_number}, {board.ply()}, """ \ 
                                      f"""{en_passant}),""" + '\n'

                    board.push(node.move)
                    node = node.next()

                if game_has_a_capture:
                    # finally execute the full query with all captures in this game
                    execute_query(connector, """INSERT INTO `chess` (`match_id`,
                        `taken_piece`, `taker_piece`, `square`, `turn`, `ply`, `en_passant`)
                         VALUES """ + '\n' + vals[:-2] + ";")

                    execute_query(connector, f"""INSERT INTO matches (`match_id`, 
                        `WhiteElo`, `BlackElo`, `ECO`, `Opening`) VALUES
                        ('{match_id}', {WhiteElo}, {BlackElo}, '{ECO}', '{Opening}'); """)

        except IndexError:
            # This error occurs when a game has no moves, it's not a problem, just skip it
            pass

        game = chess.pgn.read_game(pgn)  # get the next game and loop again
