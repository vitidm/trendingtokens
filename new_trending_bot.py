import time
import random
import mysql.connector
import requests
import json
from operator import itemgetter
from datetime import datetime, timedelta
from telegram.ext import Updater, CommandHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# Bot Credentials
updater = Updater(token='5801838689:AAGC4Zh9_S6Dt9gpf3uB3tNQ4w65XBSVi3M', use_context=True)

random_publicity_button = [
        ['Publicity here now! Visit DefiNet🤏', 'https://www.definet-platform.com/'], 
        ['Publicity here now! Visit DefiNet🤏', 'https://www.definet-platform.com/']
    ]

seconds_edit_message = 1

url = "https://api.defined.fi"
definet_lv = "NVFQLhnCYA43iooUvTJI7rLyFIWB4OQX3lIdnuj0"
definet_vd = "NVFQLhnCYA43iooUvTJI7rLyFIWB4OQX3lIdnuj0"
definet_apis = [definet_lv, definet_vd]

def getDefinedDetailedPairStats(token_pair):
    volume_type = 'hour1'
    
    headers_define = {
        "accept": "application/json",
        "x-api-key": f"{random.choice(definet_apis)}"
    }
    getDetailedPairStats = {"query": 
        """{getDetailedPairStats(pairAddress: "%s", networkId: 1, durations: %s, bucketCount: 1) 
            {
                networkId
                pairAddress
                stats_%s {
                duration
                end
                start
                statsNonCurrency {
                    buyers {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    buys {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    sellers {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    sells {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    traders {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    transactions {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                }
                statsUsd {
                    buyVolume {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    close {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    highest {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    lowest {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    open {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    sellVolume {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                    volume {
                    buckets
                    change
                    currentValue
                    previousValue
                    }
                }
                timestamps {
                    end
                    start
                }
                
            }
            }
        }""" % (token_pair, volume_type, volume_type)
    }
    response_DetailedPairStats = requests.post(url, headers=headers_define, json=getDetailedPairStats) 
    
    holders = json.loads(response_DetailedPairStats.text)['data']['getDetailedPairStats'][f'stats_{volume_type}']['statsNonCurrency']['traders']['currentValue']

    return holders

def getDefinedTokenEvents(token_pair):
    headers_define = {
        "accept": "application/json",
        "x-api-key": f"{random.choice(definet_apis)}"
    }

    getTokenEvents = """query FilterPairsQuery($token_pair: String) 
        { filterPairs(phrase: $token_pair rankings: { attribute: liquidity, direction: DESC } ) 
            {count offset results 
                { buyCount1 buyCount4 buyCount12 buyCount24 sellCount1 sellCount4 sellCount12 sellCount24 txnCount1 txnCount4 txnCount12 txnCount24 highPrice1 highPrice4 highPrice12 highPrice24 lowPrice1 lowPrice4 lowPrice12 lowPrice24 priceChange1 priceChange4 priceChange12 priceChange24 volumeUSD1 volumeUSD4 volumeUSD12 volumeUSD24 price marketCap liquidity liquidityToken exchange 
                    { address iconUrl id name networkId tradeUrl } 
                        pair { address exchangeHash fee id networkId tickSpacing token0 token1 } 
                            token0 { address cmcId decimals id name networkId symbol totalSupply } 
                                token1 { address cmcId decimals id name networkId symbol totalSupply } } } }"""
    
    variables = {"token_pair": token_pair} 

    response_tokenEvent = requests.post(url, headers=headers_define, json={"query": getTokenEvents, "variables": variables })
    response_result_filterPair =  json.loads(response_tokenEvent.text)['data']['filterPairs']['results'][0]

        
    HOLDERS = getDefinedDetailedPairStats(token_pair)
    MKT_CAP = round(float(response_result_filterPair['marketCap']), 2)
    VOL_24H = int(response_result_filterPair['volumeUSD24'])
    VOL_1H = int(response_result_filterPair['volumeUSD1'])
    TOKEN_NAME = response_result_filterPair['token0']['name']
    num_emojis = VOL_24H // 10000
    
    return MKT_CAP, VOL_24H, TOKEN_NAME, HOLDERS, num_emojis, VOL_1H
    
def sqlConnectorExtractPostTelegramTokenInfo(table_name):
    cnx = mysql.connector.connect(
        host='sql8.freesqldatabase.com',
        user='sql8593502',
        password='tuz9qrT3jT',
        database='sql8593502',
        port=3306
    )
    cursor = cnx.cursor(dictionary=True)
    current_date = datetime.now().strftime('%d/%m/%Y')
    query = f"SELECT DISTINCT(token_address), pair_address, token_name FROM all_data_tokens WHERE time LIKE '%{current_date}%'"

    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    cnx.close()

    return result

def tokenSecurity(token_address):
    url = 'https://api.gopluslabs.io/api/v1/token_security/{id}?contract_addresses={addresses}'
    id_value = '1'
    contract_addresses_value = token_address
    response = requests.get(url.format(id=id_value, addresses=contract_addresses_value))

    validator = None
    if response.status_code == 200:
        massive_data = response.json()["result"]
        
        try:
            buy_tax = float(massive_data[f"{contract_addresses_value}"]["buy_tax"])
            sell_tax = float(massive_data[f"{contract_addresses_value}"]["sell_tax"])
            lp_holders = int(massive_data[f"{contract_addresses_value}"]["lp_holder_count"])
            
            if buy_tax != 0 and buy_tax <= 0.1:
                if sell_tax != 0 and sell_tax <= 0.1:
                    if lp_holders >= 1:
                        print("buy_tax= ", buy_tax)
                        print("sell_tax= ", sell_tax)
                        
                        print("lp_holders= ", lp_holders)
                        validator = True
                        
                        return validator
                    else:
                        print("Not enough data - lp_holders")
                        validator = False
                        
                        return validator
                else:
                    print("Needs to respect sell_tax != 0 or sell_tax <= 0.1")
                    validator = False
                        
                    return validator
            else:
                print("Needs to respect buy_tax != 0 or buy_tax <= 0.1")
                validator = False
                        
                return validator
        except Exception:
            buy_tax = None
            sell_tax = None
            print("Not enough data - buy_tax, sell_tax")
            validator = False
                        
            return validator

    else:
        print("Error:", response.status_code)
        validator = False
                    
        return validator
    
def reduce_number(num):
    if num >= 1000000:
        return str(round(num / 1000000, 1)) + "M"
    elif num >= 1000:
        return str(round(num / 1000, 1)) + "K"
    else:
        return str(num)

def convert_max_day1(token):
    if token['max_day1'][-1] == 'K':
        return float(token['max_day1'][:-1]) * 1000
    elif token['max_day1'][-1] == 'M':
        return float(token['max_day1'][:-1]) * 1000000
    else:
        return float(token['max_day1'])
    
# Message template
def generate_message():
    top_10_tokens = sqlConnectorExtractPostTelegramTokenInfo("all_data_tokens")

    token_filtered = []
    for i, top_10_token in enumerate(top_10_tokens):
        validator = tokenSecurity(top_10_token['token_address'].lower())
        print(validator, top_10_token['token_name'], top_10_token['token_address'].lower())
        if validator:
            MKT_CAP, VOL_24H, TOKEN_NAME, HOLDERS, num_emojis, VOL_1H, = getDefinedTokenEvents(top_10_token['pair_address'].lower())
            if VOL_1H >= 1000:
                top_10_token['result'] = getDefinedTokenEvents(top_10_token['pair_address'].lower())
                top_10_token['max_day1'] =  reduce_number(top_10_token['result'][1])
                token_filtered.append(top_10_token)
        else:
            pass
    
    rank_list = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
    
    token_filtered = sorted(token_filtered, key=convert_max_day1, reverse=True)[:10]

    top_10_formated = []
    for i, top_10_token in enumerate(token_filtered):
        top_10_token['dextools'] = f"https://www.dextools.io/app/es/ether/pair-explorer/{top_10_token['pair_address']}"
        
        top_10_token['rank'] = rank_list[i]

    for token in token_filtered:
        top_10_formated.append(f"<b>{token['rank']}</b>" + "|" + f"<b><a href='{token['dextools']}'>{token['token_name']} </a></b>" + "|" +  f"<i>${token['max_day1']}</i>")

    top_10_formated.append("\n" + f"ℹ️ <i><a href='https://definet.fly.dev/'>DefiNet Trending</a> displays the top {len(token_filtered)} tokens that have moved the most volume during the day. </i>")

    message = "\n".join(["<b>DefiNet TOP 10 TRENDING</b>"] + top_10_formated)
    
    return message, token_filtered

def send_json_message(update, context):
    random_button = random.choice(random_publicity_button)
    
    buttons = [[InlineKeyboardButton(random_button[0], url=random_button[1])]]
    keyboard = InlineKeyboardMarkup(buttons)
    message_output, top_10_tokens = generate_message()
    original_message = context.bot.send_message(chat_id=update.message.chat_id, text=message_output, parse_mode = "HTML", reply_markup=keyboard, disable_web_page_preview=True)
    original_message_id = original_message.message_id

    # Info message
    context.bot.send_message(chat_id=update.message.chat_id, text="🎯 For every 500$ volume increase of a token in the Top-10 list, a notification will be displayed. \n \n🗣 If you wish to be featured in the advertising banner, do not hesitate to contact us at @Makimaxi.")
    
    previous_results = {}

    while True:
        time.sleep(seconds_edit_message)
        print(f"{seconds_edit_message} seconds. Message updated || {datetime.now()}")
        try:
            random_button = random.choice(random_publicity_button)
            message_output, top_10_tokens = generate_message()
            
            buttons = [[InlineKeyboardButton(random_button[0], url=random_button[1])]]
            keyboard = InlineKeyboardMarkup(buttons)

            # Edit message each X seconds
            original_message = context.bot.edit_message_text(chat_id=update.message.chat_id, message_id=original_message_id, text=message_output, parse_mode = "HTML", reply_markup=keyboard, disable_web_page_preview=True )
            original_message_id = original_message.message_id
        except:
            pass
        
        for top_10_token in top_10_tokens:
            # Obtener el nombre del token
            token_name = top_10_token['token_name']

            # Guardar el resultado actual en una variable
            current_result = {
                'volume': top_10_token['result'][1],
                'holders': top_10_token['result'][3],
                'market_cap': top_10_token['result'][0]
            }
            print(f"{top_10_token['result']}\n-----\n" , previous_results)
            # Comparar los resultados actual y anterior para determinar si se debe enviar un mensaje
            if token_name in previous_results:
                previous_volume = previous_results[token_name]['volume']
                current_volume = current_result['volume']

                if current_volume >= previous_volume + 500:
                    # Crear el mensaje de texto para enviar al grupo de Telegram
                    response_text = "<a href='https://www.dextools.io/app/es/ether/pair-explorer/{0}'>{1}</a>\n {2}\n \n📊 Total Volume: ${3}\n👥 Total Holders: {4}\n💵  Market Cap: ${5}".format(
                        top_10_token['pair_address'],
                        top_10_token['result'][2],
                        top_10_token['rank'] * top_10_token['result'][4],
                        '{:,}'.format(top_10_token['result'][1]),
                        '{:,}'.format(top_10_token['result'][3]),
                        '{:,}'.format(top_10_token['result'][0])
                    )
                    # # Agregar los links
                    response_text += f"\n\n💹 <a href='https://www.dextools.io/app/es/ether/pair-explorer/{top_10_token['pair_address']}'>Chart</a>       🦄 <a href='https://app.uniswap.org/#/tokens/ethereum/{top_10_token['token_address']}'>Buy</a>\nℹ️ <a href='https://etherscan.io/token/{top_10_token['token_address']}'>Contract</a> 📈 <a href='https://definet.fly.dev/'>DefiNet dApp</a>"
                    
                    # Enviar el mensaje
                    context.bot.send_message(chat_id=update.message.chat_id, text=response_text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)
                previous_results[token_name] = current_result
            else:
                # Guardar el resultado actual como el resultado anterior para la próxima iteración
                previous_results[token_name] = current_result
        
            time.sleep(5)

def stop(update, context):
    updater.stop()
    print("You can press Ctrl + C")
    updater.idle()
    
if __name__ == '__main__':
    updater.dispatcher.add_handler(CommandHandler('trending', send_json_message))
    updater.dispatcher.add_handler(CommandHandler('stoptrending', stop))
    updater.start_polling()