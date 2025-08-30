def get_zai_emr_system_name_version(grd_file, zai_provider_emr_mapping):
    try:
        billing_provider_tin = grd_file['billing_provider_tin'].iloc[0] if grd_file['billing_provider_tin'].iloc[0] != '' else None
    except:
        billing_provider_tin = None
    try:
        billing_provider_npi = grd_file['billing_provider_npi'].iloc[0] if grd_file['billing_provider_npi'].iloc[0] != '' else None
    except:
        billing_provider_npi = None
    if billing_provider_tin and billing_provider_tin in zai_provider_emr_mapping['provider_tin'].to_list():
        if billing_provider_npi and billing_provider_npi in zai_provider_emr_mapping['provider_npi'].to_list():
            zai_emr_system_name = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == billing_provider_tin) & \
                                                        (zai_provider_emr_mapping["provider_npi"] == billing_provider_npi)].iloc[0]['zai_emr_system_name']
            zai_emr_system_version = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == billing_provider_tin) & \
                                                        (zai_provider_emr_mapping["provider_npi"] == billing_provider_npi)].iloc[0]['zai_emr_system_version']
        else:
            zai_emr_system_name = zai_provider_emr_mapping[zai_provider_emr_mapping["provider_tin"] == billing_provider_tin].iloc[0]['zai_emr_system_name']
            zai_emr_system_version = zai_provider_emr_mapping[zai_provider_emr_mapping["provider_tin"] == billing_provider_tin].iloc[0]['zai_emr_system_version']
    else:
        zai_emr_system_name = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == "111111111") & \
                                                        (zai_provider_emr_mapping["provider_npi"] == "1111111111")].iloc[0]['zai_emr_system_name']
        zai_emr_system_version = zai_provider_emr_mapping[(zai_provider_emr_mapping["provider_tin"] == "111111111") & \
                                                        (zai_provider_emr_mapping["provider_npi"] == "1111111111")].iloc[0]['zai_emr_system_version']
    return zai_emr_system_name, zai_emr_system_version