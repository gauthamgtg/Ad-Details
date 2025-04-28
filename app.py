from curses.ascii import alt
from datetime import date, datetime, timedelta
from urllib.error import URLError
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
from functools import wraps
import pandas as pd
import hmac
import boto3
import json
import stripe
import numpy


client = boto3.client(
    "secretsmanager",
    region_name=st.secrets["AWS_DEFAULT_REGION"],
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
)

def get_secret(secret_name):
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Replace 'your-secret-name' with the actual secret name in AWS Secrets Manager
secret = get_secret("G-streamlit-KAT")
db = secret["db"]
name = secret["name"]
passw = secret["passw"]
server = secret["server"]
port = secret["port"]
stripe_key = secret["stripe"]


st.set_page_config( page_title = "Spend Stats",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded")

# st.toast('Successfully connected to the database!!', icon='😍')

st.write("Successfully connected to the database!")

def redshift_connection(dbname, user, password, host, port):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:

                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )

                cursor = connection.cursor()

                print("Connected to Redshift!")

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                print("Disconnected from Redshift!")

                return result

            except Exception as e:
                print(f"Error: {e}")
                return None

        return wrapper

    return decorator

query = '''
select buid,bid,fad_ad_account_id,fad_campaign_id,fad_adset_id,fad_ad_id,fc_picture,fad_preview_shareable_link,fad_status,fad_effective_status,fad_ad_review_feedback,fcd_objective,business_manager_id, date(fad_updated_at) as fad_updated_at
from
(
SELECT fad.ad_account_id as fad_ad_account_id,fad.campaign_id as fad_campaign_id,fad.adset_id as fad_adset_id,fad.ad_id as fad_ad_id,
fad.name as fad_name,fad.status as fad_status,fad.effective_status as fad_effective_status,fad.adcreative_id as fad_adcreative_id,
fad.preview_shareable_link as fad_preview_shareable_link,fad.ad_review_feedback as fad_ad_review_feedback,fad.created_date as fad_created_date,
fad.edited_at as fad_edited_at,fad.updated_at as fad_updated_at,fa.id as fa_id,fa.ad_id as fa_ad_id,fa.ad_name as fa_ad_name,fa.adset_id as fa_adset_id,
fa.status as fa_status,fa.created_at as fa_created_at,fa.updated_at as fa_updated_at,fa.preview_shareable_link as fa_preview_shareable_link,
fa.effective_status as fa_effective_status,facd.ad_account_id as facd_ad_account_id,facd.adcreative_id as facd_adcreative_id,facd.name as facd_name,
facd.title as facd_title,facd.message as facd_message,facd.status as facd_status,facd.object_type as facd_object_type,facd.call_to_action_type as facd_call_to_action_type,
facd.image_url as facd_image_url,facd.object_story_spec as facd_object_story_spec,facd.degrees_of_freedom_spec as facd_degrees_of_freedom_spec,
facd.thumbnail_id as facd_thumbnail_id,facd.thumbnail_url as facd_thumbnail_url,facd.video_id as facd_video_id,facd.updated_at as facd_updated_at,
facd.asset_feed_spec as facd_asset_feed_spec,fc.id as fc_id,fc.creative_id as fc_creative_id,fc.creative_name as fc_creative_name,fc.ad_id as fc_ad_id,
fc.page_id as fc_page_id,fc.picture as fc_picture,fc.message as fc_message,fc.created_at as fc_created_at,fc.updated_at as fc_updated_at,fc.video_id as fc_video_id,
fc.video_url as fc_video_url,fc.headline as fc_headline,fc.image_hash as fc_image_hash,fc.thumbnail as fc_thumbnail,fc.app_creative_id as fc_app_creative_id,
fc.object_story_id as fc_object_story_id,fc.instagram_user_id as fc_instagram_user_id,fc.instagram_media_id as fc_instagram_media_id,fc.product_set_id as fc_product_set_id,
far.id as far_id,far.object_id as far_object_id,far.level as far_level,far.error_code as far_error_code,far.error_summary as far_error_summary,
far.error_message as far_error_message,far.created_at as far_created_at,far.updated_at as far_updated_at,far.received_at as far_received_at,far.field as far_field,
far.status as far_status ,
fcd.name as fcd_name,fcd.status as fcd_status,fcd.effective_status as fcd_effective_status,fcd.objective as fcd_objective,
fcd.special_ad_categories as fcd_special_ad_categories,fcd.special_ad_category as fcd_special_ad_category,
fcd.buying_type as fcd_buying_type,fcd.created_date as fcd_created_date,fcd.edited_at as fcd_edited_at,fcd.updated_at as fcd_updated_at,
bp.buid,bp.id as bid, fcbm.business_manager_id
from zocket_global.fb_ads_details_v3 fad
left JOIN zocket_global.fb_adcreative_details_v3 facd ON facd.adcreative_id = fad.adcreative_id
LEFT JOIN zocket_global.fb_ads fa ON fa.ad_id = fad.ad_id
LEFT JOIN zocket_global.fb_creatives fc ON fa.id = fc.ad_id
left join (SELECT * from zocket_global.fb_ad_reviews
where level ='AD')far on fad.ad_id= far.object_id
left join zocket_global.fb_campaign_details_v3 fcd on fad.campaign_id=fcd.campaign_id
left join zocket_global.fb_child_ad_accounts child_acc on fad.ad_account_id=child_acc.ad_account_id
left join zocket_global.fb_child_business_managers fcbm on child_acc.app_business_manager_id=fcbm.id
left join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on fcbm.app_business_id=bp.id
where fad.ad_account_id in (SELECT distinct ad_account_id from zocket_global.fb_child_ad_accounts)
and fad.created_date>=current_date-90
)
    '''


@st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)
df = execute_query(query=query)

# st.dataframe(df, use_container_width=True)

# st.write(df.columns)
# st.dataframe(df)

st.data_editor(
    df,
    column_config={
        "fc_picture": st.column_config.ImageColumn(
            "Preview Image", help="Streamlit app preview screenshots"
        ),
        "fad_preview_shareable_link": st.column_config.LinkColumn(
            "AD Preview",
            help="View the AD preview in Facebook"
        )
        
    },
    hide_index=True,
)

ad_id = st.text_input("Enter ad_id")

st.dataframe(df[df["fad_ad_id"] == ad_id])

fad_ad_account_id = st.text_input("Enter ad_account_id")

st.dataframe(df[df["fad_ad_account_id"] == fad_ad_account_id])

filtered_df = df[['buid','bid','fad_ad_account_id','fad_campaign_id','fad_adset_id','fad_ad_id','fc_picture','fad_preview_shareable_link','fad_status','fad_effective_status','fad_ad_review_feedback','fcd_objective','business_manager_id','fad_updated_at']]


# Function to extract both components
def extract_components(json_string):
    if json_string is None:
        return None, None  # Handle None values gracefully
    try:
        # Parse the JSON string
        parsed = json.loads(json_string)
        global_value = parsed.get('global', '')
        
        # Extract the key and message from the global value
        if '=' in global_value:
            key, message = global_value.split('=', 1)
            return key.strip(), message.strip()
        else:
            return None, None  # If '=' is not present
    except (json.JSONDecodeError, KeyError, ValueError):
        return None, None  # Handle any errors gracefully

# Apply the function to extract key and message
filtered_df[['Error', 'message']] = filtered_df['fad_ad_review_feedback'].apply(
    lambda x: pd.Series(extract_components(x))
)
filtered_df['Error'] = filtered_df['Error'].str.replace(r'[0-9\(\)\[\]\.{}]', '', regex=True)

st.dataframe(filtered_df)

filtered_df = filtered_df[['buid','bid','fad_ad_account_id','fad_ad_id','fc_picture','fad_preview_shareable_link','fad_status','fad_effective_status','fad_ad_review_feedback','fcd_objective','Error','message','business_manager_id','fad_updated_at']]

filtered_df['fad_ad_account_id'] = filtered_df['fad_ad_account_id'].apply(lambda x: x.split('_')[1] if '_' in x else x)

# filtered_df['ad_acc_link']= filtered_df.apply(lambda row: f"https://adsmanager.facebook.com/adsmanager/manage/ads?act={row['fad_ad_account_id']}&business_id={row['business_manager_id']}&columns=name%2Cadgroup_id%2Cdelivery%2Crecommendations_guidance%2Ccampaign_name%2Cbid%2Cbudget%2Clast_significant_edit%2Cattribution_setting%2Cresults%2Creach%2Cimpressions%2Ccost_per_result%2Cquality_score_organic%2Cquality_score_ectr%2Cquality_score_ecvr%2Cspend%2Cend_time%2Cschedule&attribution_windows=default&filter_set=SEARCH_BY_ADGROUP_IDS-STRING_SET%1EANY%1E[%22{row['fad_ad_id']}%22]&selected_ad_ids={row['fad_ad_id']}&breakdown_regrouping=true&nav_source=no_referrer", axis=1)
filtered_df['ad_acc_link']= filtered_df.apply(lambda row: f"https://adsmanager.facebook.com/adsmanager/manage/ads?act={row['fad_ad_account_id']}&business_id={row['business_manager_id']}&columns=name%2Cadgroup_id%2Cdelivery%2Crecommendations_guidance%2Ccampaign_name%2Cbid%2Cbudget%2Clast_significant_edit%2Cattribution_setting%2Cresults%2Creach%2Cimpressions%2Ccost_per_result%2Cquality_score_organic%2Cquality_score_ectr%2Cquality_score_ecvr%2Cspend%2Cend_time%2Cschedule&attribution_windows=default&filter_set=ADGROUP_DELIVERY_INFO-STRING_SET%1EIN%1E[%22active%22%2C%22archived%22%2C%22completed%22%2C%22inactive%22%2C%22limited%22%2C%22not_delivering%22%2C%22not_published%22%2C%22pending_review%22%2C%22permanently_deleted%22%2C%22recently_completed%22%2C%22recently_rejected%22%2C%22rejected%22%2C%22scheduled%22]&breakdown_regrouping=true&nav_source=no_referrer", axis=1)

st.data_editor(
    filtered_df,
    column_config={
        "fc_picture": st.column_config.ImageColumn(
            "Preview Image", help="Streamlit app preview screenshots"
        ),
        "fad_preview_shareable_link": st.column_config.LinkColumn(
            "AD Preview",
            help="View the AD preview in Facebook"
        ),

        "ad_acc_link": st.column_config.LinkColumn(
            "ad_acc_link",
            help="View the AD in Facebook Ads Manager"
        )
        
    },
    hide_index=True,
)

grouped_df = filtered_df.groupby(['buid', 'fad_ad_account_id', 'Error']).size().reset_index(name='ErrorCount')

st.dataframe(filtered_df.groupby(['buid', 'fad_ad_account_id', 'Error']).size().reset_index(name='ErrorCount'))


grouped_df = filtered_df.groupby(['fad_updated_at','buid', 'fad_ad_account_id', 'Error']).size().reset_index(name='ErrorCount')

st.dataframe(filtered_df.groupby(['fad_updated_at','buid', 'fad_ad_account_id', 'Error']).size().reset_index(name='ErrorCount'))


st.dataframe(filtered_df[filtered_df['Error'].notna()].groupby(['buid', 'fad_ad_account_id']).size().reset_index(name='ErrorCount'))

st.dataframe(filtered_df[filtered_df['Error'].notna()].groupby(['buid']).size().reset_index(name='ErrorCount'))
