{% if var('GoogleAnalyticsEvents') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("google_analytics_events_tbl_ptrn","google_analytics_events_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {{ extract_brand_and_store_name_from_table(i, var('platform_name_position'), var('get_platform_from_tablename_flag'), var('default_platformname')) }} as platform_name,
        event_date,	
        event_timestamp,
        event_name,
        event_previous_timestamp,	
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        {{extract_nested_value("privacy_info","analytics_storage","string")}} as privacy_info_analytics_storage,
        {{extract_nested_value("privacy_info","ads_storage","string")}} as privacy_info_ads_storage,
        {{extract_nested_value("privacy_info","uses_transient_token","string")}} as privacy_info_uses_transient_token,
	
        {{extract_nested_value("user_ltv","revenue","numeric")}} as user_ltv_revenue,
        {{extract_nested_value("user_ltv","currency","string")}} as user_ltv_currency,

        {{extract_nested_value("device","category","string")}} as device_category,
        {{extract_nested_value("device","mobile_brand_name","string")}} as device_mobile_brand_name,
        {{extract_nested_value("device","mobile_model_name","string")}} as device_mobile_model_name,
        {{extract_nested_value("device","mobile_marketing_name","string")}} as device_mobile_marketing_name,
        {{extract_nested_value("device","mobile_os_hardware_model","string")}} as device_mobile_os_hardware_model,
        {{extract_nested_value("device","operating_system","string")}} as device_operating_system,
        {{extract_nested_value("device","operating_system_version","string")}} as device_operating_system_version,
        {{extract_nested_value("device","vendor_id","string")}} as device_vendor_id,
        {{extract_nested_value("device","advertising_id","string")}} as device_advertsing_id,
        {{extract_nested_value("device","language","string")}} as device_lanuage,
        {{extract_nested_value("device","is_limited_ad_tracking","string")}} as device_is_limited_ad_tarcking,
        {{extract_nested_value("device","time_zone_offset_seconds","string")}} as device_time_zone_offset_seconds,
        {{extract_nested_value("device","browser","string")}} as device_browser,
        {{extract_nested_value("device","browser_version","string")}} as device_browser_version,

        {{extract_nested_value("geo","continent","string")}} as geo_continent,
        {{extract_nested_value("geo","country","string")}} as geo_country,
        {{extract_nested_value("geo","region","string")}} as geo_region,
        {{extract_nested_value("geo","city","string")}} as geo_city,
        {{extract_nested_value("geo","sub_continent","string")}} as geo_sub_continent,
        {{extract_nested_value("geo","metro","string")}} as geo_metro,

        {{extract_nested_value("app_info","id","string")}} as app_info_id,
        {{extract_nested_value("app_info","version","string")}} as app_info_version,
        {{extract_nested_value("app_info","install_store","string")}} as app_info_install_store,
        {{extract_nested_value("app_info","firebase_app_id","string")}} as app_info_firebase_app_id,
        {{extract_nested_value("app_info","install_source","string")}} as app_info_install_source,
        
        {{extract_nested_value("traffic_source","name","string")}} as traffic_source_name,
        {{extract_nested_value("traffic_source","medium","string")}} as traffic_source_medium,
        {{extract_nested_value("traffic_source","source","string")}} as traffic_source_source,

        stream_id,
        platform,	
        {{extract_nested_value("event_dimensions","hostname","string")}} as event_dimensions_hostname,
        
        {{extract_nested_value("ecommerce","total_item_quantity","numeric")}} as ecommerce_total_item_quantity,
        {{extract_nested_value("ecommerce","purchase_revenue_in_usd","numeric")}} as ecommerce_purchase_revenue_in_usd,
        {{extract_nested_value("ecommerce","purchase_revenue","numeric")}} as ecommerce_purchase_revenue,
        {{extract_nested_value("ecommerce","refund_value_in_usd","numeric")}} as ecommerce_refund_value_in_usd,
        {{extract_nested_value("ecommerce","refund_value","numeric")}} as ecommerce_refund_value,
        {{extract_nested_value("ecommerce","shipping_value_in_usd","numeric")}} as ecommerce_shipping_value_in_usd,
        {{extract_nested_value("ecommerce","shipping_value","numeric")}} as ecommerce_shipping_value,
        {{extract_nested_value("ecommerce","tax_value_in_usd","numeric")}} as ecommerce_tax_value_in_usd,
        {{extract_nested_value("ecommerce","tax_value","numeric")}} as ecommerce_tax_value,
        {{extract_nested_value("ecommerce","unique_items","numeric")}} as ecommerce_unique_items,
        {{extract_nested_value("ecommerce","transaction_id","string")}} as ecommerce_transaction_id,

	
        {{extract_nested_value("items","item_id","string")}} as item_id,
        {{extract_nested_value("items","item_name","string")}} as item_name,
        {{extract_nested_value("items","item_brand","string")}} as item_brand,
        {{extract_nested_value("items","item_variant","string")}} as item_variant,
        {{extract_nested_value("items","item_category","string")}} as item_catagory,
        {{extract_nested_value("items","item_category2","string")}} as item_catagory2,
        {{extract_nested_value("items","item_category3","string")}} as item_catagory3,
        {{extract_nested_value("items","item_category4","string")}} as item_catagory4,
        {{extract_nested_value("items","item_category5","string")}} as item_catagory5,
        {{extract_nested_value("items","price_in_usd","numeric")}} as item_price_in_usd,
        {{extract_nested_value("items","price","numeric")}} as item_price,
        {{extract_nested_value("items","quantity","numeric")}} as item_quantity,
        {{extract_nested_value("items","item_revenue_in_usd","numeric")}} as item_revenue_in_usd,
        {{extract_nested_value("items","item_revenue","numeric")}} as item_revenue,
        {{extract_nested_value("items","item_refund_in_usd","numeric")}} as item_refund_in_usd,
        {{extract_nested_value("items","item_refund","numeric")}} as item_refund,
        {{extract_nested_value("items","coupon","string")}} as item_coupon,
        {{extract_nested_value("items","affiliation","string")}} as item_affiliation,
        {{extract_nested_value("items","location_id","string")}} as item_location_id,
        {{extract_nested_value("items","item_list_id","string")}} as item_list_id,
        {{extract_nested_value("items","item_list_name","string")}} as item_list_name,
        {{extract_nested_value("items","item_list_index","string")}} as item_list_index,
        {{extract_nested_value("items","promotion_id","string")}} as item_promotion_id,
        {{extract_nested_value("items","promotion_name","string")}} as item_promotion_name,
        {{extract_nested_value("items","creative_name","string")}} as item_creative_name,
        {{extract_nested_value("items","creative_slot","string")}} as item_creative_slot,
        
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	    from {{i}} a
                {{unnesting("privacy_info")}}
                {{unnesting("user_properties")}}
                {{multi_unnesting("user_properties", "value")}}
                {{unnesting("user_ltv")}}
                {{unnesting("device")}}
                {{unnesting("geo")}}
                {{unnesting("app_info")}}
                {{unnesting("traffic_source")}}
                {{unnesting("event_dimension")}}
                {{unnesting("ecommerce")}}
                {{unnesting("items")}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}

        WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('google_analytics_events_lookback') }},0) from {{ this }})
            {% endif %}
        qualify ROW_NUMBER() OVER (PARTITION BY event_timestamp, event_name, traffic_source_source, user_pseudo_id, ecommerce_transaction_id, device_category order by _daton_batch_runtime) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}