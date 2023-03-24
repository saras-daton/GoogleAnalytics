{% if var('GoogleAnalyticsEventsEventParams') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(event_timestamp) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%googleanalytics_%events')}}  
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% set platform_name =i.split('.')[2].split('_')[var('platform_name_position')] %}

    
    SELECT * {{exclude()}} (row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        '{{platform_name}}' as platform_name,
        event_date,	
        event_timestamp,
        event_name,
        event_params.key,
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
        {% if target.type=='snowflake' %} 
        value.VALUE:double_value :: varchar as double_value,
        value.VALUE:int_value :: varchar as int_value,
        value.VALUE:string_value :: varchar as string_value,
        device.VALUE:category :: varchar as category,
        device.VALUE:mobile_brand_name :: varchar as mobile_brand_name,
        device.VALUE:mobile_model_name :: varchar as mobile_model_name,
        device.VALUE:mobile_marketing_name :: varchar as mobile_marketing_name,
        device.VALUE:mobile_os_hardware_model :: varchar as mobile_os_hardware_model,
        device.VALUE:operating_system :: varchar as operating_system,
        device.VALUE:operating_system_version :: varchar as operating_system_version,
        device.VALUE:vendor_id :: varchar as vendor_id,
        device.VALUE:advertising_id :: varchar as advertising_id,	
        device.VALUE:language :: varchar as language,
        device.VALUE:is_limited_ad_tracking :: varchar as is_limited_ad_tracking,
        device.VALUE:time_zone_offset_seconds :: int as time_zone_offset_seconds,
        device.VALUE:browser :: varchar as browser,
        device.VALUE:browser_version :: varchar as browser_version,
        device.VALUE:web_info,
        traffic_source.VALUE:source :: varchar as source,
        traffic_source.VALUE:medium :: varchar as medium,
        traffic_source.VALUE:name :: varchar as name,
        ecommerce.VALUE:total_item_quantity :: int as total_item_quantity,
        ecommerce.VALUE:purchase_revenue_in_usd :: float as purchase_revenue_in_usd,
        ecommerce.VALUE:purchase_revenue :: float as purchase_revenue,
        ecommerce.VALUE:refund_value_in_usd :: float as refund_value_in_usd,
        ecommerce.VALUE:refund_value :: float as refund_value,
        ecommerce.VALUE:shipping_value_in_usd :: float as shipping_value_in_usd,
        ecommerce.VALUE:shipping_value :: float as shipping_value,
        ecommerce.VALUE:tax_value_in_usd :: float as tax_value_in_usd,
        ecommerce.VALUE:tax_value :: float as tax_value,
        ecommerce.VALUE:unique_items :: int as unique_items,
        ecommerce.VALUE:transaction_id :: varchar as transaction_id,
        {% else %}
        coalesce(cast(value.double_value as string),'') double_value,
        coalesce(cast(value.int_value as string),'') int_value,
        coalesce(value.string_value,'') string_value,
        device.category,	
        device.mobile_brand_name,		
        device.mobile_model_name,	
        device.mobile_marketing_name,	
        device.mobile_os_hardware_model,
        device.operating_system,
        device.operating_system_version,
        device.vendor_id,		
        device.advertising_id,		
        device.language,	
        device.is_limited_ad_tracking,	
        device.time_zone_offset_seconds,	
        device.browser,	
        device.browser_version,	
        device.web_info,	
        traffic_source.source,
        traffic_source.medium,
        traffic_source.name,
        ecommerce.total_item_quantity,	
        ecommerce.purchase_revenue_in_usd,
        ecommerce.purchase_revenue,
        ecommerce.refund_value_in_usd,
        ecommerce.refund_value,
        ecommerce.shipping_value_in_usd,
        ecommerce.shipping_value,
        ecommerce.tax_value_in_usd,
        ecommerce.tax_value,
        ecommerce.unique_items,	
        coalesce(ecommerce.transaction_id,'') transaction_id,
        {% endif %}	
        items,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY 
            event_timestamp, 
            event_name, 
            {% if target.type=='snowflake' %} 
            event_params.VALUE:key :: varchar as key,
            value.VALUE:string_value :: varchar as string_value,
            value.VALUE:int_value :: varchar as int_value,
            value.VALUE:double_value :: varchar as double_value,
            {% else %}
            event_params.key, 
            value.string_value, 
            cast(value.int_value as string), 
            cast(value.double_value as string), 
            {% endif %}	
            user_pseudo_id) row_num
	    from {{i}} a
            {{unnesting("event_params")}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.event_timestamp  >= {{max_loaded}}
            {% endif %}
        ) 
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}