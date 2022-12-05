class Chain:
    bsc_chain_id = '0x38'
    bsc_chain_name = 'BSC'
    polygon_chain_id = '0x89'
    polygon_chain_name = 'POLYGON'
    ethereum_chain_id = '0x1'
    ethereum_chain_name = 'ETHEREUM'
    ftm_chain_id = '0xfa'
    ftm_chain_name = 'FTM'
    all = [polygon_chain_id, bsc_chain_id, ethereum_chain_id, ftm_chain_id]
    mapper = {
        bsc_chain_id: bsc_chain_name,
        polygon_chain_id: polygon_chain_name,
        ethereum_chain_id: ethereum_chain_name,
        ftm_chain_id: ftm_chain_name
    }
