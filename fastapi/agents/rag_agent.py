import os
import json
from typing import List, Dict
from dotenv import load_dotenv
from langchain_milvus import Milvus
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.documents import Document
from agents.state import AgentState

from utils.logs import start_logger

# Load environment variables
load_dotenv()

# Logging
logger = start_logger()

class EmailRAGAgent:
    def __init__(self, user_email: str):
        """ Initialize the RAG agent """
        
        logger.info(f"AGENTS/RAG_AGENT - init() - Setting up RAG agent")

        self.user_email = user_email
        self.email_collection = self._format_collection_name(user_email)
        self.attachment_collection = f"{self.email_collection}_attachments"
        
        # Match the embeddings model with your existing setup
        self.embeddings = OpenAIEmbeddings(
            model       = os.getenv("EMBEDDING_MODEL"),
            api_key     = os.getenv("OPENAI_API_KEY"),
            dimensions  = 3072
        )
        
        self.llm = ChatOpenAI(
            model_name  = "gpt-4o",
            temperature = 0,
            api_key     = os.getenv("OPENAI_API_KEY"),
            top_p       = 0
        )

        self.email_vectorstore = None
        self.attachment_vectorstore = None
        self._initialize_vectorstore()
        self._setup_rag_chain()

        logger.info(f"AGENTS/RAG_AGENT - init() - Setup complete")

    def _format_collection_name(self, email: str) -> str:
        """ Format email address for collection name """

        logger.info(f"AGENTS/RAG_AGENT - _format_collection_name() - Converting email address to vector store name")
        
        return email.replace('@', os.getenv("__AT")).replace('.', os.getenv("__PERIOD"))

    def _initialize_vectorstore(self):
        """Initialize connection to Milvus vector store"""
        
        logger.info(f"AGENTS/RAG_AGENT - _initialize_vectorstore() - Setting up connection parameters for Milvus vector store server")
        
        try:
            connection_args = {
                "uri"       : f"http://{os.getenv('MILVUS_HOST')}:{os.getenv('MILVUS_PORT')}",
                "user"      : os.getenv("MILVUS_USER"),
                "password"  : os.getenv("MILVUS_PASSWORD"),
                "db_name"   : os.getenv("MILVUS_DATABASE")
            }

            # Initialize email vectorstore
            self.email_vectorstore = Milvus(
                embedding_function  = self.embeddings,
                collection_name     = self.email_collection,
                connection_args     = connection_args,
                vector_field        = "embedding",
                text_field          = "page_content"
            )
            logger.info(f"AGENTS/RAG_AGENT - _initialize_vectorstore() - Setting up parameters for email collection: {self.email_collection}")

            # Initialize attachment vectorstore
            self.attachment_vectorstore = Milvus(
                embedding_function  = self.embeddings,
                collection_name     = self.attachment_collection,
                connection_args     = connection_args,
                vector_field        = "embedding",
                text_field          = "page_content"
            )
            logger.info(f"AGENTS/RAG_AGENT - _initialize_vectorstore() - Setting up parameters for attachment collection: {self.attachment_collection}")

        except Exception as e:
            logger.error(f"AGENTS/RAG_AGENT - _initialize_vectorstore() - Failed to connect to Milvus: {e}")
            raise


    def _format_docs(self, docs: List[Document]) -> str:
        formatted_docs = []
        
        logger.info(f"AGENTS/RAG_AGENT - _format_docs() - Formatting {len(docs)} LangChain documents") 

        for doc in docs:
            metadata = doc.metadata.get("metadata", {})
            
            # Mails
            if "conversation_id" in metadata:
                formatted_docs.append(
                    f"Email:\n"
                    f"User: {metadata.get('user_email', 'N/A')}\n"
                    f"ID: {metadata.get('id', 'N/A')}\n"
                    f"Conversation ID: {metadata.get('conversation_id', 'N/A')}\n"
                    f"Conversation Index: {metadata.get('conversation_index', 'N/A')}\n"
                    f"Message Type: {metadata.get('message_type', 'N/A')}\n"
                    f"Content: {doc.page_content}\n"
                )
            
            # Mails with attachments
            elif "file_name" in metadata:
                formatted_docs.append(
                    f"Attachment:\n"
                    f"User: {metadata.get('user_id', 'N/A')}\n"
                    f"ID: {metadata.get('email_id', 'N/A')}\n"
                    f"File: {metadata.get('file_name', 'N/A')}\n"
                    f"Type: {metadata.get('file_type', 'N/A')}\n"  
                    f"Content: {doc.page_content}\n"
                )
        
        logger.info(f"AGENTS/RAG_AGENT - _format_docs() - Successfully formatted LangChain documents") 
        return "\n\n".join(formatted_docs)
    
    def _determine_query_type(self, question: str) -> Dict:
        """Determine the type and requirements of the query"""
        
        query_analysis_prompt = f"""
            Analyze the following email search query and determine its characteristics:
            Query: {question}
            
            Provide a JSON response with:
            1. primary_focus: "emails" or "attachments" or "both"
            2. time_sensitive: boolean (does query imply time relevance?)
            3. sender_specific: boolean (is query about specific senders?)
            4. requires_summarization: boolean (does response need summarization?)
            5. search_priority: "recent", "relevance", or "all"

            RESTRICTION: THE OUTPUT YOU PROVIDE WILL BE DIRECTLY FED TO json.loads() IN PYTHON. WRITE YOUR RESPONSE IN A WAY THAT json.loads() CAN HANDLE.
        """
        
        try:
            analysis = self.llm.invoke(query_analysis_prompt.format(question=question))
            return json.loads(analysis.content)
        
        except Exception as e:
            logger.error(f"AGENTS/RAG_AGENT - _determine_query_type() - Error analyzing query: {e}")
            
            return {
                "primary_focus"          : "both",
                "time_sensitive"         : False,
                "sender_specific"        : False,
                "requires_summarization" : True,
                "search_priority"        : "relevance"
            }
    
    def _combined_retrieval(self, question: str) -> str:
        """ Search both email and attachment collections """

        logger.info(f"AGENTS/RAG_AGENT - _combined_retrieval() - Attempting a combined search for emails and attachments")
        
        query_analysis = self._determine_query_type(question)
        results = []
        
        try:
            logger.info(f"AGENTS/RAG_AGENT - _combined_retrieval() - Searching for relevant emails...")

            email_k = 5 if query_analysis["primary_focus"] in ["emails", "both"] else 2
            attachment_k = 3 if query_analysis["primary_focus"] in ["attachments", "both"] else 1
            
            # Search emails
            email_retriever = self.email_vectorstore.as_retriever(
                search_kwargs={
                    "k": email_k,
                    "score_threshold": 0.65 if query_analysis["primary_focus"] == "emails" else 0.75
                }
            )

            email_results = email_retriever.invoke(question)
            for email_result in email_results:
                results.append(email_result)

            logger.info(f"AGENTS/RAG_AGENT - _combined_retrieval() - Found {len(email_results)} relevant emails")
        
        except Exception as e:
            logger.error(f"AGENTS/RAG_AGENT - _combined_retrieval() - Error searching emails: {e}")

        try:
            logger.info(f"AGENTS/RAG_AGENT - _combined_retrieval() - Searching for relevant attachments...")

            # Search attachments
            attachment_retriever = self.attachment_vectorstore.as_retriever(
                search_kwargs={
                    "k"               : attachment_k,
                    "score_threshold" : 0.65 if query_analysis["primary_focus"] == "attachments" else 0.75
                }
            )

            attachment_results = attachment_retriever.invoke(question)
            for attachment_result in attachment_results:
                results.append(attachment_result)
                    
            logger.info(f"AGENTS/RAG_AGENT - _combined_retrieval() - Found {len(attachment_results)} relevant attachments")
        
        except Exception as e:
            logger.error(f"AGENTS/RAG_AGENT - _combined_retrieval - Error searching attachments: {e}")

        return self._format_docs(results)

    def _setup_rag_chain(self):
        """Set up the RAG chain with configurable retriever"""

        logger.info(f"AGENTS/RAG_AGENT - _setup_rag_chain() - Setting up a RAG chain")
        
        prompt_template = """
        You are an intelligent email assistant with access to emails and their attachments. 
        Analyze the provided context carefully and provide a helpful, well-structured response.

        Guidelines:
        1. Focus on accuracy and relevance and limiting to email content and question
        2. If dates are mentioned in emails, include them in your response
        3. If the question is about specific senders, highlight their contributions
        4. For job-related queries, emphasize deadlines and requirements
        5. If multiple emails discuss the same topic, synthesize the information
        6. If attachments are relevant, explain their connection to the query
        7. If information is missing or unclear, explicitly state what's not available
        8. RESTRICTION: Do not generalize answer, make sure to be restrictive to the email contents, and generate answer from the context
        9. RESTRICTION: Answer should be specific to the question and email contents, do not generalize all emails and give an answer. If the answer generated is not specific to the question mention that you couldn't find specific information from email contents.

        Context:
        {context}

        Question:
        {question}

        Provide a clear, well-organized response that addresses the question directly and includes relevant details from the context. If summarizing multiple emails, structure the information logically.
        Remember, if the context of the email is not relevant to the question, simply respond by saying that the context fetched is irrelevant to the question and end your response.
        
        Response:
        """
        
        self.prompt = PromptTemplate(
            template        = prompt_template,
            input_variables = ["context", "question"]
        )

        self.rag_chain = (
            {
                "context": self._combined_retrieval,
                "question": RunnablePassthrough()
            }
            | self.prompt
            | self.llm
            | StrOutputParser()
        )

    def search(self, query: str) -> Dict:
        """Search for relevant emails and attachments"""

        logger.info(f"AGENTS/RAG_AGENT - search() - Setting up a RAG chain")
        
        try:
            response = self.rag_chain.invoke(query)
            
            return {
                "query"     : query,
                "response"  : response,
                "status"    : "success"
            }

        except Exception as e:
            logger.error(f"AGENTS/RAG_AGENT - search() - Error processing search: {e}")
            
            return {
                "query"  : query,
                "error"  : str(e),
                "status" : "error"
            }

def RagAgentNode(state: AgentState,):
    """ LangGraph Node to invoke the RAG agent """
    
    try:
        logger.info(f"AGENTS/RAG_AGENT - RagAgentNode() - Invoking RAG agent...")

        # Initialize RAG agent with user email
        user_email = state.get("user_email", None)
        query = state.get("corrected_prompt", None)

        if not user_email:
            raise(f"user_email '{user_email}'is missing to identify vector store")
        
        if not query:
            query = state.get("current_input", None)

        if not query:
            raise(f"Both 'corrected_prompt' and 'current_input' are empty!")
        
        rag_agent = EmailRAGAgent(user_email)

        logger.info(f"AGENTS/RAG_AGENT - RagAgentNode() - Starting a similarity search...")
        result = rag_agent.search(query)
        
        if result["status"] == "success":
            logger.info(f"AGENTS/RAG_AGENT - RagAgentNode() - Response generated successfully")

            state["rag_status"] = "success"
            state["rag_response"] = result['response']
        
        else:
            logger.error(f"AGENTS/RAG_AGENT - RagAgentNode() - {result['error']}")
            state["rag_status"] = "error"

    except Exception as e:
        logger.error(f"AGENTS/RAG_AGENT - RagAgentNode() - Error initializing RAG agent: {e}")

    finally:
        return state