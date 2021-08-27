import chess.pgn
import glob
from credentials import credentials
from SQL_functions import create_connection
from SQL_functions import create_database
from SQL_functions import create_connection_server
from SQL_functions import execute_query
from SQL_functions import execute_read_query
from mysql.connector import Error

# connect to the table bbl in the the database
HOST, USER, PASSWORD, DB_NAME = credentials()
connector = create_connection(HOST, USER, PASSWORD, DB_NAME)

# First drop the tables if it they are already there
try:
    drop_table = f"DROP TABLE chess"
    execute_query(connector, drop_table)
except Error as e:
    print(f"The error '{e}' occurred")

create_chess_table = """
CREATE TABLE chess (
  id INT AUTO_INCREMENT, 
  taken_piece CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin,
  taker_piece CHAR(1)  CHARACTER SET utf8 COLLATE utf8_bin,
  square INT,
  turn INT,
  en_passant BOOLEAN,
  PRIMARY KEY(id)
) ENGINE = InnoDB
"""

execute_query(connector, create_chess_table)

# Now we can populate the tables. Iterate through the matches, extracting the relevant data
for match in glob.iglob("pgn_files/*.pgn"):
    # single iteration version
    # the_match = glob.glob('bbl/524915.yaml')[0]

    pgn = open(match, 'r')
    game = chess.pgn.read_game(pgn)

    while game is not None:
        vals = ''
        try:
            if '%eval' in game.variations[0].comment:
                node = game.next()
                board = chess.Board()
                game_has_a_capture = False
                while not node.is_end():
                    en_passant = False
                    if board.is_capture(node.move):
                        game_has_a_capture = True
                        if board.piece_at(node.move.to_square) is None:
                            s = board.piece_at(node.move.from_square).symbol()
                            taken = s.lower() if s.isupper() else s.upper()
                            en_passant = True
                        else:
                            taken = board.piece_at(node.move.to_square)
                        vals = vals + f""" ('{taken}', '{board.piece_at(node.move.from_square)}', """ \
                                      f"""{node.move.to_square}, {board.fullmove_number}, {en_passant}),""" + '\n'

                    board.push(node.move)
                    node = node.next()
                if game_has_a_capture:
                    execute_query(connector,
                                  """INSERT INTO `chess` (`taken_piece`, `taker_piece`, `square`, `turn`, `en_passant`) VALUES """
                                  '\n' + vals[:-2] + ";")
        except IndexError:
            # This error occurs when a game has no moves, it's not a problem, just skip it
            pass

        game = chess.pgn.read_game(pgn)
