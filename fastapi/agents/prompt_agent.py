import json
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage
from langchain.tools import tool

from agents.state import AgentState
from utils.variables import load_env_vars
from utils.logs import start_logger
from database.connection import open_connection, close_connection
from agents.summary_agent import SummarizeEmailThread
from agents.response_agent import RespondToEmailBasedOnUserPrompt
from datetime import datetime

# Logging
logger = start_logger()

# Load env
env = load_env_vars()

@tool
def GetEmailContext(email_id: str):
    """ Fetch the respective mail's details such as subject, body, etc. """

@tool
def GeneratePromptForRAG():
    """ Based on user's input, rewrite the prompt to fetch better results from RAG """

@tool
def RetrieveFromMilvusVectorStore():
    """ Retrieve relevant emails from the Milvus Vector Store """

@tool
def DecideNextStep():
    """ After fetching the email context, decide whether to call the RAG agent or Response Agent """


def fetch_email_from_postgres(email_id):
    """ Fetch email data from Postgres to send to LLM """

    logger.info(f"AGENTS/PROMPT_AGENT - fetch_email_from_postgres() - Received request to fetch context for email ID: {email_id}")
    result = {}

    if not email_id:
        logger.error(f"AGENTS/PROMPT_AGENT - fetch_email_from_postgres() - Failed to get any email ID: {email_id}")
        return result

    conn = open_connection()
    if not conn:
        return result
    
    try:
        email_fetch_query = """
            SELECT emails.id, emails.subject, emails.body, emails.sent_datetime, emails.reply_to, senders.id, senders.name, senders.email_address, recipients.name, recipients.email_address
            FROM emails
            JOIN senders
            ON emails.id = senders.email_id
            JOIN recipients
            ON emails.id = recipients.email_id
            WHERE emails.id = %s
            LIMIT 1;
        """

        with conn.cursor() as cursor:
            cursor.execute(email_fetch_query, (email_id,))
            result = cursor.fetchone()

            if result:

                reply_to = result[4] 
                reply_to_name = None
                reply_to_address = None

                if reply_to:
                    try:
                        
                        reply_to_list = json.loads(reply_to)
                        if reply_to_list:

                            first_reply_to = reply_to_list[0].get("emailAddress")
                            if first_reply_to:
                                
                                first_reply_to_data = eval(first_reply_to)
                                reply_to_name = first_reply_to_data.get("name")
                                reply_to_address = first_reply_to_data.get("address")
                    
                    except Exception as exception:
                        logger.warning(f"AGENTS/PROMPT_AGENT - fetch_email_from_postgres() - Failed to parse reply_to: {exception}")


                email_context = {
                    "email_id"         : result[0],
                    "subject"          : result[1],
                    "body"             : result[2],
                    "sent_datetime"    : result[3].strftime('%Y-%m-%d %H:%M:%S'),
                    "reply_to_name"    : reply_to_name,
                    "reply_to_address" : reply_to_address,
                    "sender_id"        : result[5],
                    "sender_name"      : result[6],
                    "sender_email"     : result[7],
                    "recipient_name"   : result[8],
                    "recipient_email"  : result[9],
                }

                result = email_context
            
            else:
                logger.warning(f"AGENTS/PROMPT_AGENT - fetch_email_from_postgres() - No results found for email ID: {email_id}")

    except Exception as exception:
        logger.error(f"AGENTS/PROMPT_AGENT - fetch_email_from_postgres() - Exception occurred: {exception}")

    finally:
        close_connection(conn=conn)
        return result


async def GetEmailContextNode(state: AgentState):
    """ Node for fetching the email context from Postgres """
    
    logger.info("AGENTS/PROMPT_AGENT - GetEmailContextNode() - Processing email context fetch request")
    
    messages = state.get("messages", [])
    if not messages:
        logger.warning("AGENTS/PROMPT_AGENT - GetEmailContextNode() - No messages in state")
        return state
        
    # Get latest AI message
    last_message = messages[-1]
    
    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        logger.warning("AGENTS/PROMPT_AGENT - GetEmailContextNode() - Last message is not an AIMessage or has no tool calls")
        return state
        
    tool_call = last_message.tool_calls[0]
    
    if tool_call["name"] != "GetEmailContext":
        logger.warning(f"AGENTS/PROMPT_AGENT - GetEmailContextNode() - Unexpected tool call: {tool_call['name']}")
        return state
        
    try:
        # Get email ID from tool call
        email_id = tool_call["args"].get("email_id")
        
        if not email_id:
            logger.error("AGENTS/PROMPT_AGENT - GetEmailContextNode() - No email_id provided in tool arguments")
            
            error_message    = ToolMessage(
                tool_call_id = tool_call.get("id"),
                content      = "Failed to get email context: No email ID provided"
            )
            
            state["messages"].append(error_message)
            return state
            
        # Fetch email contents from Postgres
        email_context = fetch_email_from_postgres(email_id)
        
        if not email_context:
            logger.warning(f"AGENTS/PROMPT_AGENT - GetEmailContextNode() - No context found for email ID: {email_id}")
            
            error_message    = ToolMessage(
                tool_call_id = tool_call.get("id"),
                content      = f"No email found with ID: {email_id}"
            )
            
            state["messages"].append(error_message)
            return state
            
        # Add email context to state
        state["email_context"] = email_context
        
        success_message  = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content      = f"Successfully fetched email context for ID: {email_id}\nSubject: {email_context.get('subject', 'N/A')}"
        )
        state["messages"].append(success_message)
        
        logger.info(f"AGENTS/PROMPT_AGENT - GetEmailContextNode() - Successfully processed email ID: {email_id}")
        
    except Exception as exception:
        logger.error(f"AGENTS/PROMPT_AGENT - GetEmailContextNode() - Exception occurred (See details below)")
        logger.error(f"AGENTS/PROMPT_AGENT - GetEmailContextNode() - {exception}")
        
        error_message    = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content      = f"An exception occurred while fetching email context"
        )
        state["messages"].append(error_message)
    
    return state


async def DecideNextStepNode(state: AgentState):
    """ Node for deciding the next step after getting email context """
    
    logger.info("AGENTS/PROMPT_AGENT - DecideNextStepNode() - Deciding next step")

    ainvoke_kwargs = {}
    ainvoke_kwargs["parallel_tool_calls"] = False

    model = ChatOpenAI(
        model       = "gpt-4",
        temperature = 0,
        api_key     = env["OPENAI_API_KEY"]
    )

    result = await model.bind_tools(
        [
            GeneratePromptForRAG,
            RespondToEmailBasedOnUserPrompt
        ],
        **ainvoke_kwargs
    ).ainvoke([
        SystemMessage(
            content = f"""
                You are a helpful AI assistant. Your job is to call one of the provided tools. 
                
                If the user wants to:
                - Respond to the email: Use RespondToEmailBasedOnUserPrompt
                - Else: Use GeneratePromptForRAG. If not using the RespondToEmailBasedOnUserPrompt tool then you must strictly use the GeneratePromptForRAG tool
                
                User's Request: {state['current_input']}
                Email Context: {state.get('email_context', {})}
                
                Analyze the user's request and call the appropriate tool.
                You must strictly make atleast one tool call. Do not perform anything else.
            """
        )
    ])

    state["messages"].append(result)
    return state


async def GeneratePromptForRagNode(state: AgentState):
    """ Node for generating an optimized prompt for RAG agent """
    
    logger.info("AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - Processing RAG prompt generation request")
    
    messages = state.get("messages", [])
    if not messages:
        logger.warning("AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - No messages in state")
        return state
        
    # Get latest AI message
    last_message = messages[-1]
    
    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        logger.warning("AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - Last message is not an AIMessage or has no tool calls")
        return state
        
    tool_call = last_message.tool_calls[0]
    if tool_call["name"] != "GeneratePromptForRAG":
        logger.warning(f"AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - Unexpected tool call: {tool_call['name']}")
        return state
        
    try:
        # Get email context
        email_context = state.get("email_context", {})
        
        if not email_context:
            logger.error("AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - No email context available")
            error_message = ToolMessage(
                tool_call_id = tool_call.get("id"),
                content = "Failed to generate RAG prompt: No email context available"
            )
            state["messages"].append(error_message)
            return state

        model = ChatOpenAI(
            model       = "gpt-4",
            temperature = 0,
            api_key     = env["OPENAI_API_KEY"]
        )

        rag_prompt_system = f"""
            You are an AI assistant that helps generate optimal query string for vector similarity search.
            Given an email's context and/or a user's query, create a search string that will help find similar emails.
        """

        if email_context.get('subject', None):
            rag_prompt_system += f"""
                Given an email's context and a user's query, create a search string that will help find similar emails.

                Focus on key aspects like:
                1. The email's main topic and subject
                2. Key points from the email body
                3. The intent of the user's query

                User Query: {state['current_input']}
                    
                Email Context:
                Subject: {email_context.get('subject', 'N/A')}
                Body: {email_context.get('body', 'N/A')}
                Sender: {email_context.get('sender_name', 'N/A')}
            """
            
        else:
            rag_prompt_system += f"""
                Given a user's query, create a search string that will help find similar emails.

                Focus on key aspects like:
                1. The intent of the user's query.
                
                User Query: {state['current_input']}
            """
        rag_prompt_system += f"""
            Format the prompt to emphasize semantic similarity rather than exact keyword matching.
            Note that the string you generate will be directly used in vector database to find similar content. 

            Generate an optimized search string that will help find semantically similar emails.
            You must respond only with the optimized search string and nothing else.
        """

        result = await model.ainvoke([
            SystemMessage(content=rag_prompt_system),
        ])

        # Store the corrected prompt
        state["corrected_prompt"] = result.content

        # Add success message
        success_message = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content = f"Successfully generated RAG prompt: {result.content}"
        )

        state["messages"].append(success_message)
        
        logger.info("AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - Successfully generated RAG prompt")
        
    except Exception as exception:
        logger.error(f"AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - Exception occurred (See details below)")
        logger.error(f"AGENTS/PROMPT_AGENT - GeneratePromptForRagNode() - {exception}")
        
        error_message = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content = "An exception occurred while generating RAG prompt"
        )
        state["messages"].append(error_message)
    
    return state

async def StartNode(state: AgentState):
    """ Entry point for LangGraph graph """

    ainvoke_kwargs = {}
    ainvoke_kwargs["parallel_tool_calls"] = False

    model = ChatOpenAI(
        model        = "gpt-4o",
        temperature  = 0,
        api_key      = env["OPENAI_API_KEY"]
    )

    result = await model.bind_tools(
        [
            GetEmailContext, 
            GeneratePromptForRAG, 
            SummarizeEmailThread
        ],
        **ainvoke_kwargs
    ).ainvoke([
        SystemMessage(
            content = f"""
                You are a helpful AI assistant. Your job is to call the provided tools. 
                The user's input is provided in the Input field below, along with the email context if available.
                If user's input is about summary or summarization, then call the summarization tool (Presence of 'email_id' is mandatory for calling this tool).
                If the user's input is about similarity or something similar, call the generate prompt tool (Presence of 'email_id' is optional for calling this tool).

                If the Email Context contains only the 'email_id' then you must call the email context tool to fetch details of the email before you call any other tool (Presence of 'email_id' is mandatory for calling this tool).
                if 'email_id' is missing, simply call the generate prompt tool.

                Input: {state['current_input']}
                Email Context: {json.dumps(state.get('email_context', {}))}
            """
        )
    ])

    state["messages"].append(result)
    return state