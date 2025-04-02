<h2 align="center"> YouTube End-To-End Trending Videos Pipeline </h2>
<hr>

<p align="center">
  <a href="#about">About</a>
  •
  <a href="#Data Pipeline Design">Data Pipeline Design</a>
  •
  <a href="#tableau">Tableau</a>
  •
  <a href="#disclaimers">Disclaimers</a>
</p>

---

## About
An End-To-End Data Pipeline that Ingests and Transforms the Top 200 Trending YouTube Videos For 10 Countries daily. Built using Python, Microsoft Azure, Databricks, PySpark, and Tableau, this project automates data ingestion, transformation, and visualization to uncover Global Video Trends. As a longtime user of the platform, I wanted to dive deeper into the Trending Page Algorithm and answer questions like, what video categories trend more often, which creators show up consistently, and do countries share similar trending pages or vary greatly?

## Data Pipeline Design
<img width=100% src="https://imgur.com/vZafHa7.png" alt="Data Pipeline Design">
- <b>Data Source:</b> Youtube API using personal API key as Environmental Variable and connection to ADLS Gen2 Storage Containers to ingest into Microsoft Azure. <br>
- <b>Data Ingestion:</b> Automated Python Script Running Inside of an Azure Function to Scrape the Top 200 Trending Videos for 10 Countries, do Initial Data Cleaning, and convert to CSV to save in Raw Layer ADLS Gen2 Conatainer. <br>
- <b>Data Transformation:</b> Azure Data Factory runs daily to trigger Databricks PySpark Cluster to transform Raw CSV, add additional columns, and save Daily Ingestion to Transformed Layer ADLS Gen2 Conatainer. <br> 
- <b>Data Analytics:</b> Historical Database was created in Azure Synpase Analytics to develop View Tables that mimic Real World Downstream Dependencies such as Data Analysts, Data Scientists, etc. who want to query the data. <br>
- <b>Data Visulation:</b> Transformed Data flows into a Tableau Dashboard to summarize Global Video Trends from growing Historical Trending Videos Dataset. <br>

## Tableau
I developed a Tableau Dashboard to serve as an interactive visual representation that depends on the ingestion of my pipeline! This is a dashboard that is connected to my Transformed Layer ADLS Gen2 and refreshed daily. Please feel free to check it out to find out more about <a href="https://public.tableau.com/app/profile/jeremy.seals3593/viz/YoutubeDataPipelineProject/Dashboard1" target="_blank">YouTube's Trending Page on a Global Scale!</a>
<img width=100% src="https://imgur.com/IA0LmMW.png" alt="Tableau Dashboard">
<hr>


## Disclaimers
- The YouTube API has a daily credit limit that prevents abuse of the endpoint and limits the amount of times we can request trending videos. I've limited the scope of the project because of this to one batch request a day instead of multiple real time hits throughout the day to not hit that limit and would be a great enhancement for a next iteration of the project!
- Leveraging Free Version of Microsoft Azure and Tableau which grants limited access to certain features and credit limits. The project will stop being supported once these run out.
