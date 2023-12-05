{% if var('GoogleAnalyticsEventsEventParams') %}
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
        privacy_info,
        user_properties,	
        user_first_touch_timestamp,
        user_ltv,	
        geo,
        app_info,
        stream_id,
        platform,	
        event_dimensions,
        {{extract_nested_value("event_params","key","string")}} as event_params_key,
        {{extract_nested_value("value","string_value","string")}} as value_string_value,
        {{extract_nested_value("value","int_value","string")}} as value_int_value,
        {{extract_nested_value("value","float_value","string")}} as value_float_value,
        {{extract_nested_value("value","double_value","string")}} as value_double_value,

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
        {{extract_nested_value("traffic_source","name","string")}} as traffic_source_name,
        {{extract_nested_value("traffic_source","medium","string")}} as traffic_source_medium,
        {{extract_nested_value("traffic_source","source","string")}} as traffic_source_source,
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

        items,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id

	    from {{i}} a
            {{unnesting("event_params")}} 
            {{multi_unnesting("event_params", "value")}}
            {{unnesting("traffic_source")}}
            {{unnesting("device")}}
            {{unnesting("ecommerce")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}

        WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('google_analytics_events_lookback') }},0) from {{ this }})
            {% endif %}
        qualify ROW_NUMBER() OVER ( PARTITION BY  event_timestamp,event_name, event_params_key,value_string_value,value_int_value,value_double_value,user_pseudo_id order by _daton_batch_runtime) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}