use db::{dto::UserRow, users::{find_user_by_id, find_user_by_username, upsert_user}};
use chrono::Utc;

pub async fn is_user_exist(username: &str) -> Result<bool,()>{
  match find_user_by_username(username).await {
    Ok(Some(u))=>{
      Ok(true)
    },
    Ok(None)=>{
      Err(())
    },
    Err(_)=>{
      Err(())
    }
  }
}
pub async fn register_user(user_id: &str, username: &str, hashed_password: &str, salt:&str)->Result<UserRow,()>{
  match upsert_user(user_id, username, hashed_password, salt, Utc::now()).await{
    Ok(u)=>{
      Ok(u)
    },
    Err(_)=>{
      Err(())
    }
  }
}

pub async fn get_user_by_username(username: & str)->Result<Option<UserRow>,()>{
  match find_user_by_username(username).await {
    Ok(Some(u))=>{
      Ok(Some(u))
    },
    Ok(None)=>{
      Ok(None)
    },
    Err(_)=>{
      Err(())
    }
  }
}

pub async fn get_user_by_userid(userid: &str)->Result<Option<UserRow>, ()>{
  match find_user_by_id(userid).await {
  Ok(Some(u))=>{
    Ok(Some(u))
  },
  Ok(None)=>{
    Ok(None)
  },
  Err(_)=>{
    Err(())
  }
}
}