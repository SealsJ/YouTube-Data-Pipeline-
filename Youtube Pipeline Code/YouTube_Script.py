import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os, logging, time, requests, io, csv

app = func.FunctionApp()

# Get YouTube API key from environment variable
api_key = os.getenv("YOUTUBE_API_KEY")

#Azure Data Lake Storage Container Connection Setup
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "rawdata"

if not api_key or not connection_string:
    raise ValueError("Missing Youtube API Key or Azure Connection String. Check environmental variables.")

# Method to connect to Azure Data Lake Storage
def get_datalake_client(file_path):
    datalake_service_client = BlobServiceClient.from_connection_string(connection_string)
    datalake_client = datalake_service_client.get_blob_client(container=container_name, blob=file_path)
    return datalake_client

# Countries to gather top 200 trending videos from
countries = ["IN", "US", "BR", "ID", "MX", "JP", "DE", "GB", "FR", "KR"]

#characters to exclude, known to problematic with csv files
unsafe_characters = ['\n', '"']

#method to remove problematic characters in user generated fields (Channel Titles, Video Titles, Video Descriptions, etc.)
def edit_video_detail(detail):
    for i in unsafe_characters:
        detail = str(detail).replace(i, " ")
    detail = ' '.join(detail.split()) #Handeling extra spaces
    return detail

#method to handle tags so they are stored in a consistent format
def edit_tags(tags):
    return " | ".join([edit_video_detail(tag) for tag in tags]) if tags else "N/A"

#method to retrieve the top 200 videos for a given country
def get_most_popular_videos(country_code):
    videos = []
    next_page_token = None

    logging.info(f"Fetching top 200 trending videos for country code: {country_code}")

    while len(videos) < 200:
        request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,snippet,statistics,contentDetails&chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
        
        #After first response, we retrieve the nextPageToken generated and append to request_url string to get the next 50 videos, YouTube API has a max of 50 videos per request
        if next_page_token:
            request_url += f"&pageToken={next_page_token}"

        #Introduce error handeling for response request
        try: 
            response = requests.get(request_url)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return []
        
        #Error handeling for hitting the Youtube API credit limit for the day or any unforseen erros with API request
        if response.status_code != 200:
            logging.error(f"Failed to fetch videos for {country_code}. Status code: {response.status_code}, Response: {response.text}")
            return []
        
        data = response.json()
        videos.extend(data.get('items', [])) if 'items' in data else []

        next_page_token = data.get('nextPageToken', None)

        #If we reached the last page, or there isn't 200 most popular videos, we break out of the loop early and return json result
        if not next_page_token:
            break
    
    logging.info(f"Retrieved {len(videos)} videos for country code: {country_code}")
    return videos

#method to format data from JSON response and establish csv headers for each country's top 200 videos
def save_videos_to_csv(country_code, videos):
    #Get today's date for trending date column, dynamic folder and file storage
    trending_date = time.strftime("%Y-%m-%d") #YYYY-MM-DD

    # Full path for organized storage in container
    datalake_path = f"{trending_date}_Trending_Videos/{country_code}_videos.csv"

    #Establishing the CSV headers (Data we want to use in our Data Analysis later)
    headers = ["VIDEO_ID", "TITLE", "CHANNEL_TITLE", "CHANNEL_ID", "PUBLISHED_AT", "DESCRIPTION", "TAGS", "CATEGORY_ID", "DURATION", 
               "VIEW_COUNT", "LIKE_COUNT", "COMMENT_COUNT", "TRENDING_DATE", "TRENDING_DATE_RANK", "COUNTRY_CODE"]
    
    logging.info(f"Saving data to CSV: {datalake_path}")

    #Set up CSV List to save to rawsource container
    csv_data = io.StringIO()
    writer = csv.writer(csv_data, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(headers)

    #Gather video details and then add row to CSV file
    for index, video in enumerate(videos):
        snippets = video.get("snippet", {})
        statistics = video.get("statistics", {})
        contentDetails = video.get("contentDetails", {})

        video_id = video.get("id", "")
        title = edit_video_detail(snippets.get("title", ""))
        channel_title = edit_video_detail(snippets.get("channelTitle", ""))
        channel_id = snippets.get("channelId", "")
        published_at = snippets.get("publishedAt", "")
        description = edit_video_detail(snippets.get("description", ""))
        tags = edit_tags(snippets.get("tags", ["N/A"]))
        category_id = int(snippets.get("categoryId", 0))
        duration = contentDetails.get("duration", "PT0S")
        view_count = int(statistics.get("viewCount", 0))
        like_count = int(statistics.get("likeCount", 0))
        comment_count = int(statistics.get("commentCount", 0))
        rank = index + 1

        row_video_details = [video_id, title, channel_title, channel_id, published_at, description, tags, category_id, duration,
                view_count, like_count, comment_count, trending_date, rank, country_code]
        
        writer.writerow(row_video_details)
    

    # Upload the CSV file to Azure Data Lake Storage
    datalake_client = get_datalake_client(datalake_path)
    try: 
        datalake_client.upload_blob(csv_data.getvalue(), overwrite=True)  # Overwrite if the file exists
        logging.info(f"Data saved to CSV for country code: {country_code}")
    except Exception as e:
        logging.error(f"Failed to upload {datalake_path} to Azure Data Lake: {e}")

#method to run the Youtube API GET Request for all countries and then save to a csv file to upload to Data Storage
def scrape_all_countries():
    for country_code in countries:
        logging.info(f"Starting data scrape for country: {country_code}")
        try:
            most_popular_videos = get_most_popular_videos(country_code)
            save_videos_to_csv(country_code, most_popular_videos)
        except Exception as e:
            logging.error(f"Error processing country {country_code}, {e}")

@app.timer_trigger(schedule="0 10 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def GetTrendingVideos(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    # Start scraping videos for all countries
    scrape_all_countries()
