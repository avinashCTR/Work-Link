import asyncio
from couturesearch import GuardrailsUtils
import json
import pandas as pd
import httpx
from redis.asyncio import RedisCluster
import itertools
from tqdm import tqdm
import traceback

client = RedisCluster(host="10.166.181.219", port=8504, decode_responses=True)

# vertical = "ajio"
# phase = "dev06012025_0602"
# make this dynamic -> input arg from the task run
#phase = "dev09022025_1802"
cache_lifetime = 3600 * 24 * 15
# common_prefix = f"{vertical}:{phase}"
# redis_prefix = common_prefix

async def set_guardrails_filters(
    p2qh,
    query: str,
    query_expand=True,
    enable_guardrails: bool = True,
):
    # try:
        # redis_key_guardrails = f"cache:{redis_prefix}:tokenize_result:response2:{query}:{enable_guardrails}"
        # redis_key_tokenizer = (
        #     f"cache:{redis_prefix}:tokenize_result:response4:{query}:{enable_guardrails}"
        # )

        # await client.set(
        #         redis_key_guardrails,
        #         json.dumps(response[2]),
        #         ex=cache_lifetime,
        #     )
        # await client.set(
        #         redis_key_tokenizer,
        #         json.dumps(response[4]),
        #         ex=cache_lifetime,
        #     )


    response = await p2qh.tokenize(query, enable_guardrails=enable_guardrails)

    response_guardrails = response[2]
    response_tokenizer = response[4]
    open_tokens = response_guardrails["open_tokens"]
    tokenize_query = response_tokenizer["corrected"]
    token_dict = response_tokenizer["local_dict"]  # It contains Synonyms

    # print(response[3])

    # print(token_dict)
    obj = {}
    all_syn = []

    # print(response)

    obj['query'] = query
    final_tokens = response[3]

    # for i in response:
    #     print(i, '\n')


    for i in final_tokens:
        try:
            val = token_dict[i]
            # print(i)
            # print(val)
            # print(val['entities'][0])
            # print(val['token_type'])
            # print('\n')

            syn = token_dict[i].get('synonyms', [])
            synonyms = [k[0] for k in syn]
            all_syn = all_syn + synonyms
            # print(synonyms)
            ch = ['l1', 'l2', 'l3']

            # print(ch, val['token_type'])
            nch = ['brand_string_mv', 'brickprimarycolor_en_string_mv']
            if val['token_type'] in ch:
                if val['token_type'] in obj:
                    # If key exists, append to existing array
                    if isinstance(obj[val['token_type']], list):
                        obj[val['token_type']].append(i)
                    else:
                        # If it exists but is not a list, convert to list with both values
                        obj[val['token_type']] = [obj[val['token_type']], i]
                else:

                    obj[val['token_type']] = [i]
                # print(obj)
                # k = f"{val['token_type']}_syn"
            else:
                if val['token_type'] == 'corpus':
                    if(len(val['entities'])):
                        key = val['entities'][0][0]
                        obj[key] = i
                    # k = f"{key}_syn"
                    
                    if key in obj:
                            # If key exists, append to existing array
                            if isinstance(obj[key], list):
                                obj[key].append(i)
                            else:
                                # If it exists but is not a list, convert to list with both values
                                obj[key] = [obj[key], i]
                    else:
                        obj[key] = [i]
            # obj[k] = synonyms
        
        except Exception as e:
            print(f"Exception for token {i}: {e}")
            traceback.print_exc()
            continue 
    
    
    # convert them into set and lists
        all_col = ['brand_string_mv', 'brickprimarycolor_en_string_mv', 'l1', 'l2', 'l3']
        for i in all_col:
            if i in obj:
                obj[i] = list(set(obj[i]))
            
    obj['all_syn'] = all_syn        
    return obj

async def run_script(query,vertical,phase):
    try:
        util = GuardrailsUtils(client,f"{vertical}:{phase}")
        res = await set_guardrails_filters(util, query, False, True)
        return res
    finally:
        await client.aclose()

if __name__ == "__main__":
    query = "polyester blend table covers, runners & slipcovers"
    res = asyncio.run(run_script(query))
    print(res)
