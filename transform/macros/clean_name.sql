{% macro clean_name(column_name) %}
    NULLIF(
        NULLIF(
            NULLIF(
                INITCAP( 
                    TRIM(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REPLACE(REPLACE(TRIM(TRIM({{ column_name }}, '\''), '"'), '(', ' '), ')', ' '),
                                    r'(?i)\bDOCTOR\b|\bDR\.?\b', 'Dr'
                                ),
                                r'^[^a-zA-Z]+|[^a-zA-Z]+$', '' -- Strip non-letters from start/end
                            ),
                            r'\s+', ' ' -- Collapse spaces
                        )
                    )
                ), 
            'None'),
        'N/A'),
    '')
{% endmacro %}
