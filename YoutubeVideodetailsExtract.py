import os
import json
import sys
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Retrieve API key and channel ID from environment variables
# Make sure to set these in your .env file:
# YOUTUBE_API_KEY="YOUR_API_KEY"
# CHANNEL_ID="YOUR_CHANNEL_ID"
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
CHANNEL_ID = os.getenv('CHANNEL_ID')

# Validate API key and channel ID
if not YOUTUBE_API_KEY:
    print('x Error: YOUTUBE_API_KEY is missing. Please set it in your .env file.', file=sys.stderr)
    sys.exit(1)
if not CHANNEL_ID:
    print('x Error: CHANNEL_ID is missing. Please set it in your .env file.', file=sys.stderr)
    sys.exit(1)

# Initialize YouTube API client
# The 'v3' indicates the version of the YouTube Data API.
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# ---

# FUNCTION TO GET THE UPLOADS PLAYLIST ID
# This function fetches the special 'uploads' playlist ID associated with a given YouTube channel.
def get_uploads_playlist_id(channel_id):
    """
    Retrieves the uploads playlist ID for a given YouTube channel ID.

    Args:
        channel_id (str): The ID of the YouTube channel.

    Returns:
        str: The uploads playlist ID, or None if an error occurs.
    """
    try:
        # Call the channels.list API to get content details, specifically relatedPlaylists.uploads
        response = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()

        # Extract and return the uploads playlist ID
        if response['items']:
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        else:
            print(f'x Warning: No channel found for ID: {channel_id}', file=sys.stderr)
            return None
    except Exception as e:
        print(f'x Error fetching uploads playlist ID: {e}', file=sys.stderr)
        return None

# ---

# FUNCTION TO GET VIDEO IDS FROM A PLAYLIST
# This function iterates through a playlist to collect all video IDs, handling pagination.
def get_video_ids(playlist_id):
    """
    Retrieves all video IDs from a given YouTube playlist.

    Args:
        playlist_id (str): The ID of the playlist.

    Returns:
        list: A list of video IDs.
    """
    video_ids = []
    next_page_token = None

    while True:
        try:
            # Call the playlistItems.list API to get video content details
            response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,  # Max results per page is 50
                pageToken=next_page_token
            ).execute()

            # Extract video IDs from the current page and extend the list
            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])

            # Check for next page token for pagination
            next_page_token = response.get('nextPageToken')

            # If there's no next page token, we've retrieved all videos
            if not next_page_token:
                break
        except Exception as e:
            print(f'x Error fetching video IDs from playlist {playlist_id}: {e}', file=sys.stderr)
            break # Exit loop on error
    return video_ids

# ---

# FUNCTION TO FETCH VIDEO DETAILS
# This function takes a list of video IDs and fetches detailed information (title, stats, etc.) for each.
def get_video_details(video_ids):
    """
    Fetches detailed information for a list of YouTube video IDs.

    Args:
        video_ids (list): A list of video IDs.

    Returns:
        list: A list of dictionaries, each containing details for a video.
    """
    video_data = []
    
    # The YouTube Data API's videos.list method allows up to 50 IDs per request.
    # We loop in chunks of 50 to process all video IDs.
    for i in range(0, len(video_ids), 50):
        chunk_ids = video_ids[i:i + 50]
        try:
            # Call the videos.list API to get snippet (title, publishedAt) and statistics (views, likes, comments)
            response = youtube.videos().list(
                part='snippet,statistics',
                id=','.join(chunk_ids) # Join video IDs with a comma for the 'id' parameter
            ).execute()

            # Process each video item in the response
            for item in response['items']:
                video_data.append({
                    'videoId': item['id'],
                    'title': item['snippet']['title'],
                    'publishedAt': item['snippet']['publishedAt'],
                    # Use .get() with a default value to safely access statistics, as they might be missing
                    'viewCount': item['statistics'].get('viewCount', '0'),
                    'likeCount': item['statistics'].get('likeCount', '0'),
                    'commentCount': item['statistics'].get('commentCount', '0'),
                })
        except Exception as e:
            print(f'x Error fetching video details for chunk starting with {chunk_ids[0]}: {e}', file=sys.stderr)
            # Continue to the next chunk even if one fails
    return video_data

# ---

# MAIN EXTRACTION LOGIC
if __name__ == "__main__":
    print('Starting YouTube Data Extraction...')

    # 1. Get the uploads playlist ID for the specified channel
    playlist_id = get_uploads_playlist_id(CHANNEL_ID)
    if not playlist_id:
        sys.exit('Error: No uploads playlist found for the given channel ID.')

    # 2. Get all video IDs from the uploads playlist
    video_ids = get_video_ids(playlist_id)
    if not video_ids:
        sys.exit('Error: No video IDs found in the uploads playlist.')
    print(f'Found {len(video_ids)} video IDs.')

    # 3. Get detailed information for each video
    video_data = get_video_details(video_ids)
    if not video_data:
        sys.exit('Error: No video details extracted.')
    print(f'Extracted details for {len(video_data)} videos.')

    # ---

    # SAVE EXTRACTED DATA TO A FILE
    # Define the output file path. It's good practice to make the output directory if it doesn't exist.
    output_dir = '/home/luxde/youtube' # Changed path for better compatibility/testing. Adjust if needed.
    output_file_name = 'youtube_videos_raw.json'
    output_file_path = os.path.join(output_dir, output_file_name)

    # Create the output directory if it doesn't already exist
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Write the extracted video data to a JSON file
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(video_data, f, indent=4, ensure_ascii=False)
        print(f'Successfully extracted data and saved to {output_file_path}')
    except IOError as e:
        print(f'x Error writing data to file {output_file_path}: {e}', file=sys.stderr)

    print('YouTube Data Extraction Complete!')
