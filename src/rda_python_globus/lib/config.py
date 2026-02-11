import os

""" Globus Client IDs """
QUASAR_CLIENT_ID = "05c2f58b-c667-4fc4-94fb-546e1cd8f41f"
TACC_CLIENT_ID = "3320e8d9-030c-41d0-94f0-2357ae2e3e4d"

""" Token storage configuration """
CLIENT_TOKEN_CONFIG = '/glade/u/home/gdexdata/globus/globus_gdex_quasar_tokens.json'
TACC_TOKEN_CONFIG = '/glade/u/home/gdexdata/globus/globus_tacc_transfer_tokens.json'

""" Log file path and name """
GDEX_BASE_PATH = '/glade/campaign/collections/gdex'
SCRATCH_PATH = '/lustre/desc1/scratch/tcram'
LOGPATH = os.path.join(SCRATCH_PATH, 'logs/globus')
LOGFILE = 'dsglobus-app.log'

""" Endpoint IDs """
RDA_DATASET_ENDPOINT = 'b6b5d5e8-eb14-4f6b-8928-c02429d67998'
RDA_DSRQST_ENDPOINT = 'e6cd9f43-935c-42e3-8d19-764d03241719'
RDA_STRATUS_ENDPOINT = 'be4aa6a8-9e35-11eb-8a8e-d70d98a40c8d'
RDA_GLADE_ENDPOINT = '7f0acd80-dfb2-4412-b7b5-ebc970bedf24'
RDA_QUASAR_ENDPOINT = 'e50caa88-feae-11ea-81a2-0e2f230cc907'
RDA_QUASAR_DR_ENDPOINT = '4c42c32c-feaf-11ea-81a2-0e2f230cc907'
GLOBUS_CGD_ENDPOINT_ID = '11651c26-80c2-4dac-a236-7755530731ac'

GDEX_DATASET_ENDPOINT = 'c4e40965-a024-43d7-bef4-6010f3731b61'
GDEX_DSRQST_ENDPOINT = 'e6cd9f43-935c-42e3-8d19-764d03241719'
GDEX_STRATUS_ENDPOINT = RDA_STRATUS_ENDPOINT  # same as RDA Stratus
GDEX_OS_ENDPOINT = '558ad782-80dd-4656-a64a-2245f38a7c9e' # GDEX Boreas S3 guest collection
GDEX_GLADE_ENDPOINT = '039e1667-8a6c-4cbd-8e26-1f86c72f6e89'
GDEX_QUASAR_ENDPOINT = RDA_QUASAR_ENDPOINT  # same as RDA Quasar
GDEX_QUASAR_DR_ENDPOINT = RDA_QUASAR_DR_ENDPOINT  # same as RDA Quasar DR
GDEX_LUSTRE_ENDPOINT = 'f95ce17e-d4ec-458d-8853-225456f4a09b' # GDEX Lustre guest collection (/lustre/desc1/gdex)

TACC_GLOBUS_ENDPOINT = '57c4032a-2b50-47f0-adf8-13fff3a7d77d'
TACC_BASE_PATH = '/scoutfs/projects/ASC26015'

""" Endpoint aliases """
ENDPOINT_ALIASES = {
    "rda-glade": RDA_GLADE_ENDPOINT,
    "rda-quasar": RDA_QUASAR_ENDPOINT,
    "rda-quasar-drdata": RDA_QUASAR_DR_ENDPOINT,
    "rda-dataset": RDA_DATASET_ENDPOINT,
    "rda-dsrqst": RDA_DSRQST_ENDPOINT,
    "rda-stratus": RDA_STRATUS_ENDPOINT,
    "gdex-glade": GDEX_GLADE_ENDPOINT,
    "gdex-quasar": GDEX_QUASAR_ENDPOINT,
    "gdex-quasar-drdata": GDEX_QUASAR_DR_ENDPOINT,
    "gdex-dataset": GDEX_DATASET_ENDPOINT,
    "gdex-dsrqst": GDEX_DSRQST_ENDPOINT,
    "gdex-stratus": GDEX_STRATUS_ENDPOINT,
    "gdex-os": GDEX_OS_ENDPOINT,
    "gdex-boreas": GDEX_OS_ENDPOINT,
    "cgd": GLOBUS_CGD_ENDPOINT_ID,
    "tacc": TACC_GLOBUS_ENDPOINT,
    "gdex-lustre": GDEX_LUSTRE_ENDPOINT
}
