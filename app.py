from pandasql import sqldf
import pandas as pd
import sqlite3
import csv

connection = sqlite3.connect('atp2023.db')
cursor = connection.cursor()
select_query = ''

create_table = '''
               CREATE TABLE match_stats (
               tourney_id,
               tourney_name,
               surface,
               draw_size,
               tourney_level,
               tourney_date,
               match_num,
               winner_id,
               winner_seed,
               winner_entry,
               winner_name,
               winner_hand,
               winner_ht,
               winner_ioc,
               winner_age,
               loser_id,
               loser_seed,
               loser_entry,
               loser_name,
               loser_hand,
               loser_ht,
               loser_ioc,
               loser_age,
               score,
               best_of,round,
               minutes,
               w_ace,
               w_df,
               w_svpt,
               w_1stIn,
               w_1stWon,
               w_2ndWon,
               w_SvGms,
               w_bpSaved,
               w_bpFaced,
               l_ace,
               l_df,
               l_svpt,
               l_1stIn,
               l_1stWon,
               l_2ndWon,
               l_SvGms,
               l_bpSaved,
               l_bpFaced,
               winner_rank,
               winner_rank_points,
               loser_rank,
               loser_rank_points
               );
  '''


try:
    cursor.execute(create_table)
except sqlite3.DatabaseError:
    print("Table already created")


connection.commit()

matches_2023 = pd.read_csv('Data/atp_matches_2023.csv')

file = open('Data/atp_matches_2023.csv')
statistics = csv.reader(file)

insert = ''' INSERT INTO match_stats
               (tourney_id,
               tourney_name,
               surface,
               draw_size,
               tourney_level,
               tourney_date,
               match_num,
               winner_id,
               winner_seed,
               winner_entry,
               winner_name,
               winner_hand,
               winner_ht,
               winner_ioc,
               winner_age,
               loser_id,
               loser_seed,
               loser_entry,
               loser_name,
               loser_hand,
               loser_ht,
               loser_ioc,
               loser_age,
               score,
               best_of,round,
               minutes,
               w_ace,
               w_df,
               w_svpt,
               w_1stIn,
               w_1stWon,
               w_2ndWon,
               w_SvGms,
               w_bpSaved,
               w_bpFaced,
               l_ace,
               l_df,
               l_svpt,
               l_1stIn,
               l_1stWon,
               l_2ndWon,
               l_SvGms,
               l_bpSaved,
               l_bpFaced,
               winner_rank,
               winner_rank_points,
               loser_rank,
               loser_rank_points)
                VALUES
                (?,?,?,?,?,?,?,?,?,?,
                 ?,?,?,?,?,?,?,?,?,?,
                 ?,?,?,?,?,?,?,?,?,?,
                 ?,?,?,?,?,?,?,?,?,?,
                 ?,?,?,?,?,?,?,?,?)
                '''
               
cursor.executemany(insert, statistics)
connection.commit()


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



winner_lost_bagel = '''SELECT winner_name, loser_name, score, tourney_name, round
                              FROM match_stats WHERE score LIKE '%0-6%'
                            '''




# Prompt user to search for specific data from csv file
try: 
  choice = int(input('''
                  View data were:
                   1. Player outside top 10 beat a top 10 player
                   2. Top 10 player won against player outside top 10
                   3. Most hard court wins
                   4. Most clay court wins
                   5. Most grass court wins
                   6. Matches where winner lost a bagel set (0-6)
                   7. All matches were top 10 player lost
                   8. Most tournament wins
                   9. Finals for every tournament
                     
                   Choice:   
'''))
  


except ValueError:
   print("Please choose a number between 1 and 9")
else:
   if choice == 1:
      select_query = higher_rank_lost
   elif choice == 2:
      select_query = lower_rank_lost
   elif choice == 3:
      select_query = most_hard_wins
   elif choice == 4:
      select_query = most_clay_wins
   elif choice == 5:
      select_query = most_grass_wins
   elif choice == 6:
      select_query = winner_lost_bagel
   elif choice == 7:
      select_query = top_10_losses
   elif choice == 8:
      select_query = most_tourney_wins
   elif choice == 9:
      select_query = finals_scores
      
rows = cursor.execute(select_query).fetchall()

print("Results: ")
for r in rows:
   print(r)

connection.commit()
connection.close()