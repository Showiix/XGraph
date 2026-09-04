"""X Web GraphQL operations used by the first XGraph collector slice.

Adapted from twscrape/api.py at upstream commit
55ac729f39fbbe46e316746a55627a9ed920112c (MIT).
"""

import json
from typing import Any

from xgraph.domain import Operation

GQL_URL = "https://x.com/i/api/graphql"

OPERATION_PATHS = {
    Operation.USER_BY_SCREEN_NAME: "Gb-d6r0vxPOADdG62OEBpQ/UserByScreenName",
    Operation.FOLLOWING: "qGZZDF3mp91q7X22s3HxpA/Following",
    Operation.USER_TWEETS: "SXVCYB8XHSS25nzIljNtZA/UserTweets",
}

GQL_FEATURES = {
    "articles_preview_enabled": False,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "communities_web_enable_tweet_community_results_fetch": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "premium_content_api_read_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": False,
    "responsive_web_edit_tweet_api_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_grok_analysis_button_from_backend": False,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": False,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_grok_image_annotation_enabled": False,
    "responsive_web_grok_imagine_annotation_enabled": False,
    "responsive_web_grok_share_attachment_enabled": False,
    "responsive_web_jetfuel_frame": False,
    "responsive_web_media_download_video_enabled": False,
    "responsive_web_profile_redirect_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "responsive_web_grok_show_grok_translated_post": True,
    "rweb_tipjar_consumption_enabled": True,
    "rweb_video_screen_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_awards_web_tipping_enabled": False,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "tweet_with_visibility_results_prefer_gql_media_interstitial_enabled": False,
    "tweetypie_unmention_optimization_enabled": True,
    "verified_phone_label_enabled": False,
    "view_counts_everywhere_api_enabled": True,
}

USER_LOOKUP_FEATURES = {
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "hidden_profile_likes_enabled": True,
    "hidden_profile_subscriptions_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "profile_label_improvements_pcf_label_in_post_enabled": False,
    "responsive_web_twitter_article_notes_tab_enabled": False,
    "subscriptions_feature_can_gift_premium": False,
    "subscriptions_verification_info_is_identity_verified_enabled": False,
    "subscriptions_verification_info_verified_since_enabled": True,
}


def operation_url(operation: Operation) -> str:
    return f"{GQL_URL}/{OPERATION_PATHS[operation]}"


def encode_params(variables: dict[str, Any], features: dict[str, bool]) -> dict[str, str]:
    return {
        "variables": json.dumps(variables, separators=(",", ":")),
        "features": json.dumps({**GQL_FEATURES, **features}, separators=(",", ":")),
    }
