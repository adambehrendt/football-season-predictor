import requests
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np


class DataScraper():
	def __init__(self, seasons, directory):
		"""
		Initializes DataScraper class.

		Args:
			seasons: List, the years that we want to get data for.
			directory: String, where the raw data will be stored.
		"""
		# set_up is a structure necessary for pulling both passing and rushing data from both the offense and defense
		self.set_up = [['null','TEAM_PASSING'], ['null','RUSHING'], ['TEAM_PASSING','null'], ['RUSHING','null']]
		self.seasons = seasons
		self.make_data_directory(directory)

	def make_data_directory(self, directory):
		"""
		Checks if the directory exists and if not, creates it.

		Args:
			directory: String, where the raw data will be stored.
		"""
		if not os.path.exists(directory):
			print ('Creating directory: {}.'.format(directory))
			os.makedirs(directory)
		else:
			print('Directory: {}, already exists.'.format(directory))

	def scrape_teams(self):
		"""
		Scrapes team data from nfl.com. Specifically this in total season information about each team that year.
		For example: The 2009 Buffalo Bills with X passing yards per game, Y% passer rating, etc.
		"""
		count = 0
		# Instantiate an empty DataFrame to store our final result.
		dataframe = pd.DataFrame()
		# Loop through seasons
		for season in self.seasons:
			# Instantiate list to store a DataFrame for each set_up type
			df_list = [None for _ in range(len(self.set_up))]
			count = 0
			# Loop through set_up, allowing us to get offensive and defensive stats for both passing and rushing.
			for value in self.set_up:
				if value[0] == 'null':
					prefix = "off_{}".format(value[1])
				else:
					prefix = "def_{}".format(value[0])
				url = 'http://www.nfl.com/stats/categorystats?archive=false&conference=null&role=TM&offensiveStatisticCategory={offense}&defensiveStatisticCategory={defense}&season={season}&seasonType=REG&tabSeq=2&qualified=false&Submit=Go'.format(offense=value[0], defense=value[1], season=season)
				# Get the html and parse it
				response = requests.get(url)
				soup = BeautifulSoup(response.text,'html.parser')
				# Find the table in the html
				table_text = soup.find("table")
				# Get each row of the table
				table = table_text.findAll(lambda tag: tag.name=='tr')
				header = table[0]
				data = table[1:]
				# Make column names
				col_names = [prefix+entry.get_text().replace('\n','') if entry.get_text().replace('\n','') != 'Team' else 'Team' for entry in header.findAll(lambda tag: tag.name=='th')]
				# Instantiate empty list of lists to store the final data
				rows = [[None for i in range(len(col_names))] for j in range(len(data))]
				for i in range(len(data)):
					# Split each row into a list of entries
					row = data[i].findAll(lambda tag: tag.name=='td')
					for j in range(len(row)):
						# Get the text from each entry, remove the whitespace and add it to our rows vector.
						rows[i][j] = row[j].get_text().replace('\t', '').replace('\n', '').replace(',', '')
				# Create a DataFrame for this specifc set_up type and add it to df_list
				data_dict = {x:list(y) for x,y in zip(col_names, zip(*rows))}
				df = pd.DataFrame.from_dict(data_dict)
				df_list[count] = df
				count += 1
			# Merge the data of each set_up type into one DataFrame
			curr_df = df_list[0]
			for i in range(len(df_list)-1):
				curr_df = pd.merge(curr_df, df_list[i+1], on ='Team', how = 'inner')
			# Add a column specifying the season and add it to the desired final DataFrame
			curr_df['season'] = season
			dataframe = dataframe.append(curr_df)
		# Export to .csv
		dataframe.to_csv('raw_data/TeamSeasonStats_Raw.csv')

	def scrape_games(self):
		"""
		Scrapes the game results for seasons specified from pro-football-reference.com.
		For example: Week 1, Date, Buffalo Bills Won, Patriots Lost, Patriots Away-Team, Score, etc...
		"""
		for season in self.seasons:
			url = 'https://www.pro-football-reference.com/years/{season}/games.htm'.format(season=season)
			# Get the html and parse it
			response = requests.get(url)
			soup = BeautifulSoup(response.text,'html.parser')
			# Find the table in the html
			table_text = soup.find("table")
			# Get all the rows in the table
			table = table_text.findAll(lambda tag: tag.name=='tr')
			header = table[0]
			data = table[1:]
			# Make the column names
			col_names = [entry.get_text().replace('\n','') for entry in header.findAll(lambda tag: tag.name=='th')]
			# Create the list of lists to store the final data for this season (accounting for a constant 256 games per regular season)
			rows = [[None for i in range(len(col_names))] for j in range(256)]
			count = 0
			for i in range(len(data)):
				row = data[i].findAll(lambda tag: tag.name=='td' or tag.name=='th')
				indicator = row[2].get_text()
				# If we hit the playoffs, end iterating through the table
				if indicator == 'Playoffs':
					break
				# If we hit a sub-header in our traversal of the table move to the next row
				elif indicator == 'Date':
					continue
				else:
					for j in range(len(row)):
						# Get the text from each entry, remove the whitespace and add it to our rows vector.
						rows[count][j] = row[j].get_text().replace('\t', '').replace('\n', '').replace(',', '')
					# Only counting when we add a real data point, not a sub_header
					count += 1
			# Put the data into a DataFrame and export to .csv
			data_dict = {x:list(y) for x,y in zip(col_names, zip(*rows))}
			df = pd.DataFrame.from_dict(data_dict)
			df.to_csv("raw_data/GameData_{}_Raw.csv".format(season))

	def scrape_data(self):
		"""
		Scrapes the team data, then the game data and saves them to their associated .csv's.
		"""
		print('Scraping offensive and defensive team statistics from nfl.com for seasons {first}-{last}...'.format(first=self.seasons[0], last=self.seasons[-1]))
		self.scrape_teams()
		print("Done.")
		print('Scraping game results from pro-football-reference.com for seasons {first}-{last}...'.format(first=self.seasons[0], last=self.seasons[-1]))
		self.scrape_games()
		print("Done.")
