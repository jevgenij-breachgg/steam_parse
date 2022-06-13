import requests
import json
import pandas as pd
import mysql.connector
import logging
import warnings
import os
import configparser

logging.basicConfig(filename='errors.log',
                    filemode='a',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.WARNING)

log = logging.FileHandler('errors.log')
warnings.simplefilter(action='ignore', category=FutureWarning)


# get data (hours by game) from Steam API
def get_from_steam(uuid):

    base_url = 'https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/' \
               '?key=949DDA17D1A94CB9D387F2BE0727EB5F&steamid='
    full_url = base_url + uuid
    tmp_df = pd.DataFrame()
    try:
        response_api = requests.get(full_url)
        data = json.loads(response_api.text)
        tmp_df = pd.json_normalize(data)
    except requests.exceptions.RequestException as e:
        print("Element not found")
        logging.error("Steampowered API error: {}".format(e))

    try:
        df = pd.json_normalize(tmp_df['response.games'][0])
    except BaseException as e:
        logging.error("JSON normalization error: {}".format(e))
        logging.error("User data is private. User ID: {}".format(uuid))
        return pd.DataFrame()

    df = df.drop(df.columns.difference(['playtime_forever', 'appid']), 1, inplace=False)
    df = df.rename(columns={"appid": "app_id", "playtime_forever": "play_time"})
    df['steam_account_id'] = uuid

    return df


# get game data and tags from steamspy
def get_from_steamspy(app_id):

    base_url = 'https://steamspy.com/api.php?request=appdetails&appid='
    full_url = base_url + app_id
    tmp_df = pd.DataFrame()

    try:
        response_api = requests.get(full_url)
        data = json.loads(response_api.text)
        tmp_df = pd.json_normalize(data['tags'])
        name = data['name']
        tmp_df = tmp_df.transpose()
    except requests.exceptions.RequestException as e:
        print("Element not found")
        logging.error("Steamspy API error: {}".format(e))
        name = ''

    if not tmp_df.empty:
        tmp_df.index.names = ['tag_name']
        tmp_df = tmp_df.reset_index()
    else:
        tmp_df = pd.DataFrame(columns=['tag_name'])
        tmp_df = tmp_df.append({'tag_name': 'tags'}, ignore_index=True)

    tmp_df['app_id'] = app_id
    tmp_df['game_name'] = name
    tmp_df = tmp_df.drop(tmp_df.columns.difference(['tag_name', 'app_id', 'game_name']), 1, inplace=False)
    tmp_df = tmp_df.dropna(axis=0, subset=['game_name'])

    return tmp_df


# DB connector
def db_con():

    # DB connection data from Config.ini parsing
    dir_name = os.path.dirname(__file__)
    config_path = os.path.join(dir_name, 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    config = config

    # DB connection values definition
    host = config['mysql']['host']
    database = config['mysql']['database']
    user = config['mysql']['user']
    password = config['mysql']['password']

    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    return mydb


# Get requested users from DB
def check_user_hub(connection):

    sql = """
    Select steam_account_id From steam_user.hub_user where update_request = 1
    """
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print("error on check_user_hub: {}".format(err))
        logging.error("error on check_user_hub: {}".format(err))
        connection.close()

    result = cursor.fetchall()

    return result


# Transform List to query
def list_to_query(listing):

    str1 = 'select "' + str(listing[0]) + '" as chk_value '

    for ele in listing:
        str1 += ' UNION SELECT "'
        str1 += str(ele)
        str1 += '" '
    return str1


# check if new games appear
def check_game_hub(df, connection):

    write_new_to_db(df, 'tmp_game', connection)

    sql = """
        SELECT tg.app_id AS app_id
            FROM steam_user.tmp_game tg
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM steam_user.hub_game hg
                    WHERE tg.app_id = hg.app_id
                    )
            GROUP BY tg.app_id
        """
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except mysql.connector.Error as err:
        print("error on check_user_hub: {}".format(err))
        logging.error("error on check_user_hub: {}".format(err))
        result = []
        connection.close()
        return result

    return result


# check if new tags appear
def check_tag_hub(sql, connection):

    sql = " WITH A AS ( " + sql + " ) SELECT DISTINCT A.chk_value as tag_name " \
                                  "FROM A WHERE NOT EXISTS " \
                                  "( SELECT 1 FROM hub_tag hg WHERE A.chk_value = hg.tag_name) "
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print("error on check_tag_hub: {}".format(err))
        logging.error("error on check_tag_hub: {}".format(err))
        connection.close()

    result = cursor.fetchall()

    return result


# write new information to MySQL
def write_new_to_db(df, table_name, connection):

    cursor = connection.cursor()
    cols = "`,`".join([str(i) for i in df.columns.tolist()])

    try:
        for i, row in df.iterrows():
            sql = "INSERT INTO `" + table_name + "` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cursor.execute(sql, tuple(row))
    except mysql.connector.Error as err:
        print("error on load: {}".format(err))
        logging.error("error on load: {}".format(err))
        connection.close()

    try:
        connection.commit()
    except mysql.connector.Error as err:
        print("error on commit: {}".format(err))
        logging.error("error on commit: {}".format(err))
        connection.close()


# Update hub_user if acc is private
def update_user_hub_private(uuid, connection):

    cursor = connection.cursor()
    uuid = [uuid]
    try:
        sql = "UPDATE hub_user SET private = 1 , update_request = 0 WHERE steam_account_id = %s"
        cursor.execute(sql, uuid)
    except mysql.connector.Error as err:
        print("error on update: {}".format(err))
        logging.error("error on update: {}".format(err))
        connection.close()

    try:
        connection.commit()
    except mysql.connector.Error as err:
        print("error on commit: {}".format(err))
        logging.error("error on commit: {}".format(err))
        connection.close()


# truncate table
def truncate_db(table_name, connection):

    cursor = connection.cursor()
    truncate = "truncate " + table_name

    try:
        cursor.execute(truncate)
    except mysql.connector.Error as err:
        print("error on truncate: {}".format(err))
        logging.error("error on truncate: {}".format(err))
        connection.close()

    try:
        connection.commit()
    except mysql.connector.Error as err:
        print("error on commit: {}".format(err))
        logging.error("error on commit: {}".format(err))
        connection.close()


# update hub_user with new stats
def call_user_hub_update(uuid, connection):

    cursor = connection.cursor()
    call = "CALL steam_user.ufn_user_stat( '" + uuid + "') ;"

    try:
        cursor.execute(call)
    except mysql.connector.Error as err:
        print("error on CALL: {}".format(err))
        logging.error("error on CALL: {}".format(err))
        connection.close()

    try:
        connection.commit()
    except mysql.connector.Error as err:
        print("error on commit: {}".format(err))
        logging.error("error on commit: {}".format(err))
        connection.close()


# Main function
def main():

    connection = db_con()
    truncate_db('user_activity', connection)
    truncate_db('tmp_game', connection)
    uuid = check_user_hub(connection)

    for user in uuid:
        print(user[0])
        user_act = get_from_steam(user[0])

        if not user_act.empty:
            app_df = pd.DataFrame(user_act['app_id'], columns=['app_id'])
            app_id = check_game_hub(app_df, connection)
            if app_id:
                for a in app_id:
                    df = get_from_steamspy(str(a[0]))
                    if not df.empty:
                        df2 = df.groupby(by=['app_id', 'game_name']).nunique()
                        df2 = df2.reset_index()
                        df2 = df2.drop(df2.columns.difference(['app_id', 'game_name']), 1, inplace=False)
                        write_new_to_db(df2, 'hub_game', connection)

                        df3 = df.groupby(by=['tag_name']).nunique()
                        df3 = df3.reset_index()
                        df3 = df3.drop(df3.columns.difference(['tag_name']), 1, inplace=False)
                        tag_id_list = df3['tag_name'].tolist()
                        tag_name = check_tag_hub(list_to_query(tag_id_list), connection)

                        if tag_name:
                            tag_df = pd.DataFrame(tag_name, columns=['tag_name'])
                            write_new_to_db(tag_df, 'hub_tag', connection)

                        df4 = df.groupby(by=['app_id', 'tag_name']).nunique()
                        df4 = df4.reset_index()
                        df4 = df4.drop(df4.columns.difference(['app_id', 'tag_name']), 1, inplace=False)
                        write_new_to_db(df4, 'link_game_to_tag', connection)

            write_new_to_db(user_act, 'user_activity', connection)
            call_user_hub_update(str(user[0]), connection)
        else:
            update_user_hub_private(user[0], connection)

        truncate_db('user_activity', connection)
        truncate_db('tmp_game', connection)

    connection.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
