# --- This is the main entry point for the AI ---
# --- It defines the workflow graph and the entry point for the agent ---

import json
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from typing import Dict, List, Optional, Union, cast
from langchain_core.messages import AIMessage, ToolMessage, BaseMessage

from agents.state import AgentState

# Node imports
from agents.prompt_agent import StartNode, GetEmailContextNode, DecideNextStepNode, GeneratePromptForRagNode, SummarizeEmailThreadNode, RespondToEmailNode

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
workflow.add_node("respond_to_email", RespondToEmailNode)
workflow.add_node("decide_next_step", DecideNextStepNode)

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
            
    if isinstance(messages[-1], ToolMessage):
        if len(messages) >= 2:
            
            previous_message = messages[-2]
            if (
                isinstance(previous_message, AIMessage) and 
                previous_message.tool_calls and 
                previous_message.tool_calls[0]["name"] == "GetEmailContext"
            ):
                print("REACHED HERE")
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
workflow.add_conditional_edges("start_node", route, ["get_email_context", "summarize_thread", END])
workflow.add_conditional_edges("get_email_context", route, ["decide_next_step", END])
workflow.add_conditional_edges("decide_next_step", route, ["generate_rag_prompt", "summarize_thread", "respond_to_email", END])

# Attach RAG agent to this node
workflow.add_conditional_edges("generate_rag_prompt", route, [END])

workflow.add_edge("summarize_thread", END)
workflow.add_edge("respond_to_email", END)

# Compile workflow
graph = workflow.compile(checkpointer=memory)


async def process_input(
    user_input: str,
    email_context: Optional[Dict] = None,
    message_history: Optional[List[BaseMessage]] = None,
) -> Dict:
    """Process user input through the workflow."""
    
    initial_state = AgentState(
        messages         = message_history or [],
        current_input    = user_input,
        email_context    = email_context or {},
        corrected_prompt = None,
    )
    
    final_state = await graph.ainvoke(initial_state, config=config_dict)
    
    # return {
    #     "corrected_prompt": final_state.get("corrected_prompt", None),
    # }

    return final_state


if __name__ == "__main__":
    import asyncio
    
    async def main():
        result = await process_input(
            user_input="Can you show me emails similar to this one?",
            email_context={
                "email_id": "AAMkADAwNDhkZDI3LThlODMtNDNkNy04ZGRjLWQwN2I1N2UxNjAyMABGAAAAAACDoa9wVVCtSYVLL53u_ueBBwBQwg2UQmqmTYVgJHd2DKqdAAAAAAEMAABQwg2UQmqmTYVgJHd2DKqdAAGAhattAAA="
            }
        )
        print(result)
    
    asyncio.run(main())