import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(sp, *args, **kwargs):
    sp['track_id'] = sp.index

    released_dim = pd.DataFrame(columns=['released_id','released_year','released_month','released_day'])
    released_dim['released_year'] = sp['released_year']
    released_dim['released_month'] = sp['released_month']
    released_dim['released_day'] = sp['released_day']
    released_dim = released_dim.drop_duplicates().reset_index(drop='True')
    released_dim['released_id'] = released_dim.index
   
    playlist_dim = pd.DataFrame(columns=['playlist_id','in_spotify_playlists','in_apple_playlists','in_deezer_playlists'])
    playlist_dim['in_spotify_playlists'] = sp['in_spotify_playlists']
    playlist_dim['in_apple_playlists'] = sp['in_apple_playlists']
    playlist_dim['in_deezer_playlists'] = sp['in_deezer_playlists']
    playlist_dim = playlist_dim.drop_duplicates().reset_index(drop='True')
    playlist_dim['playlist_id'] = playlist_dim.index

    charts_dim = pd.DataFrame(columns=['chart_id','in_spotify_charts','in_apple_charts','in_deezer_charts', 'in_shazam_charts'])
    charts_dim['in_spotify_charts'] = sp['in_spotify_charts']
    charts_dim['in_apple_charts'] = sp['in_apple_charts']
    charts_dim['in_deezer_charts'] = sp['in_deezer_charts']
    charts_dim['in_shazam_charts'] = sp['in_shazam_charts']
    charts_dim = charts_dim.drop_duplicates().reset_index(drop='True')
    charts_dim['chart_id'] = charts_dim.index

    song_dim = pd.DataFrame(columns=['song_id','danceability_%','valence_%','energy_%', 'acousticness_%', 'instrumentalness_%','liveness_%','speechiness_%'])
    song_dim['danceability_%'] = sp['danceability_%']
    song_dim['valence_%'] = sp['valence_%']
    song_dim['energy_%'] = sp['energy_%']
    song_dim['acousticness_%'] = sp['acousticness_%']
    song_dim['instrumentalness_%'] = sp['instrumentalness_%']
    song_dim['liveness_%'] = sp['liveness_%']
    song_dim['speechiness_%'] = sp['speechiness_%']
    song_dim = song_dim.drop_duplicates().reset_index(drop='True')
    song_dim['song_id'] = song_dim.index

    fact_table =  sp.merge(released_dim, on=['released_year', 'released_month', 'released_day']) \
                .merge(song_dim, on=['danceability_%','valence_%','energy_%', 'acousticness_%', 'instrumentalness_%','liveness_%','speechiness_%']) \
                .merge(playlist_dim, on=['in_spotify_playlists','in_apple_playlists','in_deezer_playlists']) \
                .merge(charts_dim, on=['in_spotify_charts','in_apple_charts','in_deezer_charts', 'in_shazam_charts'] ) \
                [['track_id','released_id','song_id','playlist_id','chart_id','track_name', 'artist(s)_name', 'artist_count','streams', 'bpm','key', 'mode']]

    fact_table['streams'] = fact_table['streams'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    fact_table['streams'] = pd.to_numeric(fact_table['streams'], errors='coerce')



    fact_table = fact_table.rename(columns={'artist(s)_name': 'artist_name'})

    return {"released_dim":released_dim.to_dict(orient="dict"),
    "playlist_dim":playlist_dim.to_dict(orient="dict"),
    "charts_dim":charts_dim.to_dict(orient="dict"),
    "song_dim":song_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
