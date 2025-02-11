use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Cloud {
    Aws,
    Azure,
    Gcp,
    // TODO(tamer): restrictions and validation on cloud name.
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Region {
    pub cloud: Option<Cloud>,
    pub name: String,
}

impl From<Region> for proto::universe::Region {
    fn from(val: Region) -> Self {
        proto::universe::Region {
            cloud: val.cloud.map(|c| match c {
                Cloud::Aws => {
                    proto::universe::region::Cloud::PublicCloud(proto::universe::Cloud::Aws.into())
                }
                Cloud::Azure => proto::universe::region::Cloud::PublicCloud(
                    proto::universe::Cloud::Azure.into(),
                ),
                Cloud::Gcp => {
                    proto::universe::region::Cloud::PublicCloud(proto::universe::Cloud::Gcp.into())
                }
                Cloud::Other(s) => proto::universe::region::Cloud::OtherCloud(s),
            }),
            name: val.name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Zone {
    pub region: Region,
    pub name: String,
}

impl From<Zone> for proto::universe::Zone {
    fn from(val: Zone) -> Self {
        proto::universe::Zone {
            region: Some(val.region.into()),
            name: val.name,
        }
    }
}

impl fmt::Display for Cloud {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Aws => "aws",
            Self::Azure => "azure",
            Self::Gcp => "gcp",
            Self::Other(s) => s,
        };
        write!(f, "{}", str)
    }
}

impl FromStr for Cloud {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO(tamer): unit test to ensure cloud round-trips from string.
        match s {
            "aws" => Ok(Self::Aws),
            "azure" => Ok(Self::Azure),
            "gcp" => Ok(Self::Gcp),
            _ => Ok(Self::Other(s.to_string())),
        }
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cloud {
            None => write!(f, "{}", self.name),
            Some(c) => write!(f, "{}:{}", c, self.name),
        }
    }
}

impl FromStr for Region {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split(":").collect();
        match parts.len() {
            1 => Ok(Region {
                cloud: None,
                name: s.to_string(),
            }),
            2 => {
                let cloud = parts[0].parse::<Cloud>()?;
                Ok(Region {
                    cloud: Some(cloud),
                    name: parts[1].to_string(),
                })
            }
            _ => Err("invalid region string".to_string()),
        }
    }
}

impl Serialize for Region {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct RegionStringVisitor;

impl Visitor<'_> for RegionStringVisitor {
    type Value = Region;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representation of the region")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // TODO(tamer): remove unwrap
        Ok(value.parse::<Region>().unwrap())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(value.to_string())
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D>(deserializer: D) -> Result<Region, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(RegionStringVisitor)
    }
}

impl fmt::Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.region, self.name)
    }
}

impl FromStr for Zone {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split("/").collect();
        if parts.len() != 2 {
            return Err("Invalid zone string".to_string());
        }
        let region = parts[0].parse::<Region>()?;
        Ok(Zone {
            region,
            name: parts[1].to_string(),
        })
    }
}

impl Serialize for Zone {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct ZoneStringVisitor;

impl Visitor<'_> for ZoneStringVisitor {
    type Value = Zone;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representation of the zone")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // TODO(tamer): remove unwrap
        Ok(value.parse::<Zone>().unwrap())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(value.to_string())
    }
}

impl<'de> Deserialize<'de> for Zone {
    fn deserialize<D>(deserializer: D) -> Result<Zone, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ZoneStringVisitor)
    }
}
