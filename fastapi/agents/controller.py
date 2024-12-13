# --- This is the main entry point for the AI ---
# --- It defines the workflow graph and the entry point for the agent ---

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from typing import Dict, List, Optional, cast
from langchain_core.messages import AIMessage, ToolMessage, BaseMessage

from agents.state import AgentState

# Node imports
from agents.prompt_agent import StartNode, GetEmailContextNode, DecideNextStepNode, GeneratePromptForRagNode
from agents.rag_agent import RagAgentNode
from agents.summary_agent import SummarizeEmailThreadNode
from agents.response_agent import RespondToEmailNode

from utils.variables import load_env_vars
from utils.logs import start_logger


# Logging
logger = start_logger()

# Load env
env = load_env_vars()

# Define the workflow
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("start_node", StartNode)
workflow.add_node("get_email_context", GetEmailContextNode)
workflow.add_node("generate_rag_prompt", GeneratePromptForRagNode)
workflow.add_node("summarize_thread", SummarizeEmailThreadNode)
workflow.add_node("decide_next_step", DecideNextStepNode)
workflow.add_node("rag_agent", RagAgentNode)
workflow.add_node("respond_to_email", RespondToEmailNode)

def route(state):
    """ Route to determine the next node to call """

    messages = state.get("messages", [])
    
    if messages and isinstance(messages[-1], AIMessage):
        ai_message = cast(AIMessage, messages[-1])
        print("AI RESPONSE", ai_message)

        if ai_message.tool_calls:
            tool_name = ai_message.tool_calls[0]["name"]
            print("Calling tool:", tool_name)

            if tool_name == "GetEmailContext":
                return "get_email_context"
            
            elif tool_name == "GeneratePromptForRAG":
                return "generate_rag_prompt"
            
            elif tool_name == "SummarizeEmailThread":
                return "summarize_thread"
            
            elif tool_name == "RespondToEmailBasedOnUserPrompt":
                return "respond_to_email"
            
    if isinstance(messages[-1], ToolMessage):
        if len(messages) >= 2:
            
            previous_message = messages[-2]
            if (
                isinstance(previous_message, AIMessage) and 
                previous_message.tool_calls and 
                previous_message.tool_calls[0]["name"] == "GetEmailContext"
            ):
                return "decide_next_step"
    
    return END

# Conversation memory
memory = MemorySaver()

config_dict = {
    "thread_id": "prompt_correction",
    "checkpoint_ns": "prompt_correction_ns",
    "checkpoint_id": "session"
}

# Connect nodes
workflow.set_entry_point("start_node")
workflow.add_conditional_edges("start_node", route, ["get_email_context", "generate_rag_prompt", "summarize_thread", END])
workflow.add_conditional_edges("get_email_context", route, ["decide_next_step", END])
workflow.add_conditional_edges("decide_next_step", route, ["generate_rag_prompt", "respond_to_email", END])

# Attach RAG agent to this node
workflow.add_edge("generate_rag_prompt", "rag_agent")
workflow.add_edge("rag_agent", END)

workflow.add_edge("summarize_thread", END)
workflow.add_edge("respond_to_email", END)

# Compile workflow
graph = workflow.compile(checkpointer=memory)


async def process_input(
    user_input      : str,
    user_email      : Optional[str],
    email_context   : Optional[Dict] = None,
    message_history : Optional[List[BaseMessage]] = None,
) -> Dict:
    """ Process user input through the workflow """
    
    initial_state = AgentState(
        messages             = message_history or [],
        current_input        = user_input,
        email_context        = email_context or {},
        user_email           = user_email or None,
        corrected_prompt     = None,
        rag_status           = None,
        rag_response         = None,
        conversation_summary = None,
        response_output      = None
    )
    
    final_state = await graph.ainvoke(initial_state, config=config_dict)

    return {
        "current_input"        : final_state.get("current_input", None),
        "email_context"        : final_state.get("email_context", None),
        "user_email"           : final_state.get("user_email", None),
        "corrected_prompt"     : final_state.get("corrected_prompt", None),
        "rag_status"           : final_state.get("rag_status", None),
        "rag_response"         : final_state.get("rag_response", None),
        "conversation_summary" : final_state.get("conversation_summary", None),
        "response_output"      : final_state.get("response_output", None)
    }

