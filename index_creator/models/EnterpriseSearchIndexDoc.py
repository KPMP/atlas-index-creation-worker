class EnterpriseSearchIndexDoc:
    def __init__(self, access, platform, experimental_strategy, data_category, workflow_type, data_format,
                 data_type, file_id, file_name, file_size, package_id, dois, redcap_id, sample_type, tissue_type,
                 protocol, sex, age_binned, tissue_source):
        self.access = access
        self.platform = platform
        self.experimental_strategy = experimental_strategy
        self.data_category = data_category
        self.workflow_type = workflow_type
        self.file_id = file_id
        self.file_name = file_name
        self.data_format = data_format
        self.file_size = file_size
        self.data_type = data_type
        self.package_id = package_id
        self.dois = dois
        self.redcap_id = redcap_id
        self.sample_type = sample_type
        self.tissue_type = tissue_type
        self.protocol = protocol
        self.sex = sex
        self.age_binned = age_binned
        self.tissue_source = tissue_source
        self.participant_id_sort = redcap_id[0].replace('-', '') if len(redcap_id) == 1 else "Multiple Participants"
        self.file_name_sort = file_name[37:]
        self.platform_sort = "aaaaa" if not platform else platform
