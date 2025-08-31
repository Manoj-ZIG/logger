import json
from typing import Dict, List, Any, Optional
import os

def convert_geometry(geometry: Dict) -> Dict:
    """Convert geometry structure to match Java format."""
    if not geometry:
        return None
        
    return {
        "boundingBox": {
            "width": geometry["BoundingBox"]["Width"],
            "height": geometry["BoundingBox"]["Height"],
            "left": geometry["BoundingBox"]["Left"],
            "top": geometry["BoundingBox"]["Top"]
        },
        "polygon": [
            {
                "x": point["X"],
                "y": point["Y"]
            }
            for point in geometry["Polygon"]
        ]
    }

def convert_relationships(relationships: List) -> Optional[List]:
    """Convert relationships to match Java format."""
    if not relationships:
        return None
        
    return [
        {
            "type": rel["Type"].lower(),
            "ids": rel["Ids"]
        }
        for rel in relationships
    ]

def convert_block(block: Dict) -> Dict:
    """Convert a single block to match Java format."""
    return {
        "blockType": block["BlockType"],
        "confidence": block.get("Confidence"),
        "text": block.get("Text"),
        "rowIndex": None,
        "columnIndex": None,
        "rowSpan": None,
        "columnSpan": None,
        "geometry": convert_geometry(block.get("Geometry")),
        "id": block["Id"],
        "relationships": convert_relationships(block.get("Relationships")),
        "entityTypes": None,
        "selectionStatus": None,
        "page": block["Page"]
    }

def convert_format(python_json: Dict) -> List:  # Changed return type to List
    """Convert entire JSON structure from Python to Java format."""
    # Initialize as a list instead of an object
    converted_blocks = []
    
    # Process all blocks from all entries in the Python JSON
    for entry in python_json:
        if "Blocks" in entry:
            for block in entry["Blocks"]:
                converted_blocks.append(convert_block(block))
    
    return converted_blocks  # Return just the array of blocks