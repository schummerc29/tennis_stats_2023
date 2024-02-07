from pandasql import sqldf
import pandas as pd

matches_2023 = pd.read_csv('Data/atp_matches_2023.csv')

# Pulls matches where higher ranked opponent won
higher_rank_lost = sqldf('''SELECT winner_name, loser_name 
            FROM matches_2023 WHERE winner_rank > loser_rank''')

# Pulls matches where higher ranked opponent won
lower_rank_lost = sqldf('''SELECT winner_name, loser_name 
            FROM matches_2023 WHERE winner_rank < loser_rank''')

# Calculates top players with most Hard court wins
most_hard_wins = sqldf('''SELECT winner_name, count(*) AS Hard_Court_Wins 
                       from matches_2023 WHERE surface = 'Hard' 
                       GROUP BY winner_name ORDER BY Hard_Court_Wins DESC
                       LIMIT 10''')

# Calculates top players with most Clay court wins
most_clay_wins = sqldf('''SELECT winner_name, count(*) AS Clay_Court_Wins 
                       from matches_2023 WHERE surface = 'Clay' 
                       GROUP BY winner_name ORDER BY Clay_Court_Wins DESC''')

# Calculates top players with most Grass court wins
most_grass_wins = sqldf('''SELECT winner_name, count(*) AS Grass_Court_Wins 
                       from matches_2023 WHERE surface = 'Grass' 
                       GROUP BY winner_name ORDER BY Grass_Court_Wins DESC''')

# Returns players and scores from each tournament this year
finals_scores = sqldf('''SELECT winner_name, loser_name, 
                             tourney_name, score
                             FROM matches_2023 WHERE round = 'F' 
                            ORDER BY tourney_date''')

# Returns all matches where the winner lost a set 0-6
winner_lost_bagel_set = sqldf('''SELECT winner_name, loser_name, score, tourney_name, round
                              FROM matches_2023 WHERE score LIKE '%0-6%'
                            ''')

# All matches where a top 10 players lost to a player outside the top 10 at the time the match was played
top_10_losses = sqldf('''SELECT  
                      winner_name AS 'Winner', FLOOR(winner_rank) AS 'Winner Rank', 
                      loser_name AS 'Loser', FLOOR(loser_rank) AS 'Loser Rank',
                      score AS 'Score', tourney_name AS 'Tournament', round AS 'Round'
                             FROM matches_2023 WHERE loser_rank <= 11 AND winner_rank > 10
                            ''')

# All matches that ended in a player retirement
retired_finishes = sqldf('''SELECT 
                          winner_name AS 'Winner', loser_name AS 'Loser',
                          score AS 'Score', tourney_name AS 'Tournament',
                          round AS 'Round' FROM matches_2023
                          WHERE score LIKE '%RET'
                          ''')

# List players with the most tournament wins in 2023
most_tourney_wins = sqldf('''SELECT winner_name AS 'Champion', count(*) AS 'Tournaments Won' 
                          FROM matches_2023 WHERE round = 'F'GROUP BY winner_name
                          ORDER BY count(*) DESC
                          ''')


# Generate Dataframe for all query result sets
higher_rank_lost_report = pd.DataFrame(higher_rank_lost)
lower_rank_lost_report = pd.DataFrame(lower_rank_lost)
hard_wins_report = pd.DataFrame(most_hard_wins)
clay_wins_report = pd.DataFrame(most_clay_wins)
grass_wins_report = pd.DataFrame(most_grass_wins)
bagel_winners = pd.DataFrame(winner_lost_bagel_set)
top_losses_report = pd.DataFrame(retired_finishes)
most_titles = pd.DataFrame(most_tourney_wins)
finals_report = pd.DataFrame(finals_scores)


higher_rank_lost_report.to_csv('higher_ranked_player_lost.txt', sep='|', index=False)
lower_rank_lost_report.to_csv('lower_ranked_player_lost.txt', sep='|', index=False)
hard_wins_report.to_csv('hard_court_wins.txt', sep='|', index=False)
clay_wins_report.to_csv('clay_court_wins.txt', sep='|', index=False)
grass_wins_report.to_csv('grass_court_wins', sep='|', index=False)
bagel_winners.to_csv('bagel_winners.txt', sep='|', index=False)
top_losses_report.to_csv('losses_by_top_10.txt', sep='|', index=False)
most_titles.to_clipboard('most_tourney_wins.txt', sep='|', index=False)
finals_report.to_csv('finals_scores.txt', sep='|', index=False)