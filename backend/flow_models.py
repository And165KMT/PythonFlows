from __future__ import annotations
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator


class NodeModel(BaseModel):
    id: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    x: Optional[float] = 0
    y: Optional[float] = 0
    w: Optional[float] = 220
    prevH: Optional[float] = 140
    params: Dict[str, Any] = Field(default_factory=dict)


class EdgeModel(BaseModel):
    from_: str = Field(..., alias='from', min_length=1)
    to: str = Field(..., min_length=1)

    class Config:
        allow_population_by_field_name = True


class GroupFrameModel(BaseModel):
    x: float
    y: float
    w: float
    h: float


class GroupModel(BaseModel):
    id: str
    name: str
    nodeIds: List[str] = Field(default_factory=list)
    collapsed: Optional[bool] = False
    frame: Optional[GroupFrameModel] = None


class ViewModel(BaseModel):
    scale: float = 1
    tx: float = 0
    ty: float = 0


class FlowModel(BaseModel):
    version: int = 1
    nodes: List[NodeModel] = Field(default_factory=list)
    edges: List[EdgeModel] = Field(default_factory=list)
    nextId: int = 1
    groups: List[GroupModel] = Field(default_factory=list)
    view: Optional[ViewModel] = None
    activePkg: Optional[str] = None

    @validator('edges', each_item=True)
    def _edge_nodes_exist(cls, e: EdgeModel, values: Dict[str, Any]):
        node_ids = {n.id for n in values.get('nodes', [])}
        if e.from_ not in node_ids or e.to not in node_ids:
            raise ValueError(f"edge references unknown node: {e.from_} -> {e.to}")
        return e
