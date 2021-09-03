SELECT * FROM chess.chess LIMIT 20;

-- Amount of captures by piece
SELECT upper(taker_piece) piece, COUNT(*) num_captures
FROM chess.chess
GROUP BY upper(taker_piece)
ORDER BY COUNT(upper(taker_piece)) DESC;

-- Amount of captures by each colour
SELECT A.colour colour, COUNT(*) num_captures
FROM (
	SELECT CASE WHEN taker_piece=lower(taker_piece)
	THEN "black"
    ELSE "white"
    END colour
    FROM chess.chess
    ) A
GROUP BY A.colour
ORDER BY count(*) DESC;