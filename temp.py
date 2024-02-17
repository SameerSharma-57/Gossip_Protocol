# # import os
# # import json



# # l = [{"type": "peer_Request", "ip": "127.0.0.1", "port": 56827},{"type": "message", "data": "bye", "time": "Fri Feb 16 12:27:59 2024"} ]

# # #get size in bytes of the objects in l
# # for x in l:
# #     print(len(json.dumps(x).encode('utf-8')))
# import logging
# from colorlog import ColoredFormatter

# output_file = f'output.log'
# MESSAGE = 6
# PEER_REQUEST = 7
# PEER_REPLY = 8
# DEATH = 9
# LIVELINESS = 10
# # Add new log level to the logger
# logging.addLevelName(MESSAGE,'message')
# logging.addLevelName(PEER_REQUEST,'peer_Request')
# logging.addLevelName(PEER_REPLY,'peer_Reply')
# logging.addLevelName(DEATH,'Death')
# logging.addLevelName(LIVELINESS,'Liveliness')
# # Create logger and set level to DEBUG
# logger = logging.getLogger('my_logger')

# # Create console handler and set level to DEBUG
# console_handler = logging.StreamHandler()


# # Create colored formatter for console handler
# console_formatter = ColoredFormatter(
#     "%(log_color)s%(asctime)s - %(message)s",
#     log_colors={
#         'DEBUG': 'reset',
#         'INFO': 'reset',
#         'WARNING': 'yellow',
#         'ERROR': 'red',
#         'CRITICAL': 'bold_red',
#         'message':'green',
#         'peer_Request':'blue',
#         'peer_Reply':'cyan',
#         'Death':'bold_red',
#         'Liveliness':'white'
#     }
# )
# console_handler.setFormatter(console_formatter)

# # Add console handler to the logger
# logger.addHandler(console_handler)

# # Create file handler and set level to DEBUG
# file_handler = logging.FileHandler(output_file)

# # Create colored formatter for file handler
# file_formatter = ColoredFormatter(
#     '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
#     log_colors={
#         'DEBUG': 'reset',
#         'INFO': 'reset',
#         'WARNING': 'yellow',
#         'ERROR': 'red',
#         'CRITICAL': 'bold_red',
#         'message':'green',
#         'peer_Request':'blue',
#         'peer_Reply':'cyan',
#         'Death':'bold_red',
#         'Liveliness':'white'
#     }
# )
# file_handler.setFormatter(file_formatter)

# # Add file handler to the logger
# logger.addHandler(file_handler)

# # Now, you can log messages to both console and the file with different colors for INFO level
# logger.setLevel('message')
# logger.log(MESSAGE,"Debug message")  # This will have a different color in the file
# logger.setLevel('Death')
# logger.log(DEATH,"Informational message")  # This will have a different color in the file
# logger.setLevel('peer_Reply')
# logger.log(PEER_REQUEST,"Warning message")  # This will have a different color in the file

# # Close the file handler to ensure all logs are written
# file_handler.close()
import logging, colorlog
TRACE = 5
logging.addLevelName(TRACE, 'TRACE')
formatter = colorlog.ColoredFormatter(log_colors={'TRACE': 'yellow'})
handler = logging.FileHandler('output.log')
handler.setFormatter(formatter)
logger = logging.getLogger('example')
logger.addHandler(handler)
logger.setLevel('TRACE')
logger.log(TRACE, 'a message using a custom level')
