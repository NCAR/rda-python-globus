import os

""" RDA Quasar Client ID """
CLIENT_ID = "05c2f58b-c667-4fc4-94fb-546e1cd8f41f"

""" Token storage configuration """
CLIENT_TOKEN_CONFIG = '/glade/u/home/rdadata/lib/python/globus_rda_quasar_tokens.json'

""" Log file path and name """
RDA_BASE_PATH = '/glade/campaign/collections/gdex'
SCRATCH_PATH = '/lustre/desc1/scratch/tcram'
LOGPATH = os.path.join(SCRATCH_PATH, 'logs/globus')
LOGFILE = 'dsglobus-app.log'

""" Endpoint IDs """
RDA_DATASET_ENDPOINT = 'b6b5d5e8-eb14-4f6b-8928-c02429d67998'
RDA_DSRQST_ENDPOINT = 'e1e2997e-d794-4868-838e-d4b8d5590853'
RDA_STRATUS_ENDPOINT = 'be4aa6a8-9e35-11eb-8a8e-d70d98a40c8d'
RDA_GLADE_ENDPOINT = '7f0acd80-dfb2-4412-b7b5-ebc970bedf24'
RDA_QUASAR_ENDPOINT = 'e50caa88-feae-11ea-81a2-0e2f230cc907'
RDA_QUASAR_DR_ENDPOINT = '4c42c32c-feaf-11ea-81a2-0e2f230cc907'
GLOBUS_CGD_ENDPOINT_ID = '11651c26-80c2-4dac-a236-7755530731ac'

GDEX_DATASET_ENDPOINT = 'c4e40965-a024-43d7-bef4-6010f3731b61'
GDEX_DSRQST_ENDPOINT = '0104eacd-94b4-4202-9966-817247f0ca8c'
GDEX_STRATUS_ENDPOINT = RDA_STRATUS_ENDPOINT  # same as RDA Stratus
GDEX_GLADE_ENDPOINT = '039e1667-8a6c-4cbd-8e26-1f86c72f6e89'
GDEX_QUASAR_ENDPOINT = RDA_QUASAR_ENDPOINT  # same as RDA Quasar
GDEX_QUASAR_DR_ENDPOINT = RDA_QUASAR_DR_ENDPOINT  # same as RDA Quasar DR

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
    "cgd": GLOBUS_CGD_ENDPOINT_ID,
}
