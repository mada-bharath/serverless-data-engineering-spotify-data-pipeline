import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO



def albums(data):
    album_list = []
    for row in data.get("items", []):
        track = row.get("track")
        if not track:
            continue

        album = track.get("album", {})
        album_list.append({
            "album_id": album.get("id"),
            "album_name": album.get("name"),
            "release_date": album.get("release_date"),
            "tracks": album.get("total_tracks"),
            "url": album.get("external_urls", {}).get("spotify")
        })
    return album_list


def artists(data):
    artist_details = []
    for row in data.get("items", []):
        track = row.get("track")
        if not track:
            continue

        for artist in track.get("artists", []):
            artist_details.append({
                "artist_id": artist.get("id"),
                "artist_name": artist.get("name"),
                "artist_url": artist.get("external_urls", {}).get("spotify")
            })
    return artist_details


def songs(data):
    song_list = []
    for row in data.get("items", []):
        track = row.get("track")
        if not track:
            continue

        song_list.append({
            "song_id": track.get("id"),
            "song_name": track.get("name"),
            "duration_ms": track.get("duration_ms"),
            "url": track.get("external_urls", {}).get("spotify"),
            "popularity": track.get("popularity"),
            "song_added": row.get("added_at"),
            "album_id": track.get("album", {}).get("id"),
            "artist_id": track.get("album", {}).get("artists", [{}])[0].get("id")
        })
    return song_list




def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotify-etl-project-123"
    key = "raw_data/to_processed/"

    spotify_data = []
    spotify_keys = []

    for file in s3.list_objects(Bucket=Bucket, Prefix=key).get("Contents", []):
        if file["Key"].endswith(".json"):
            obj = s3.get_object(Bucket=Bucket, Key=file["Key"])
            spotify_data.append(json.loads(obj["Body"].read()))
            spotify_keys.append(file["Key"])

    all_albums, all_artists, all_songs = [], [], []

    for data in spotify_data:
        all_albums.extend(albums(data))
        all_artists.extend(artists(data))
        all_songs.extend(songs(data))

    album_df = pd.DataFrame(all_albums).drop_duplicates("album_id")
    artist_df = pd.DataFrame(all_artists).drop_duplicates("artist_id")
    song_df = pd.DataFrame(all_songs).drop_duplicates("song_id")

    song_key = f"transformed_data/songs_data/song_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    buffer = StringIO()
    song_df.to_csv(buffer, index=False)
    s3.put_object(Bucket=Bucket, Key=song_key, Body=buffer.getvalue())
    
    album_key = f"transformed_data/album_data/album_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    buffer = StringIO()
    album_df.to_csv(buffer, index=False)
    s3.put_object(Bucket=Bucket, Key=album_key, Body=buffer.getvalue())
    
    artist_key = f"transformed_data/airtist_data/artist_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    buffer = StringIO()
    artist_df.to_csv(buffer, index=False)
    s3.put_object(Bucket=Bucket, Key=artist_key, Body=buffer.getvalue())



    s3_resource = boto3.client("s3")

    for key in spotify_keys:

        if "to_processed/" not in key:
            print(f" Skipping (wrong prefix): {key}")
            continue

        destination_key = key.replace("to_processed/", "processed/")

        print(" Copying:")
        print(f"   FROM: {key}")
        print(f"   TO  : {destination_key}")

        
        s3.copy_object(
            Bucket=Bucket,
            CopySource={"Bucket": Bucket, "Key": key},
            Key=destination_key
        )

       
        s3.delete_object(
            Bucket=Bucket,
            Key=key
        )
