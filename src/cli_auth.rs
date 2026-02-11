#![cfg(feature = "auth")]

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use serde_json::json;
use std::path::Path;
use streamline::{
    Acl, AclFilter, AclStore, Operation, PatternType, Permission, ResourceType, Result, User,
    UserStore,
};

use super::{AclCommands, UserCommands};
use crate::cli_context::CliContext;
use crate::OutputFormat;

pub(super) fn handle_user_command(cmd: UserCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        UserCommands::List { users_file } => {
            list_users(&users_file, ctx)?;
        }
        UserCommands::Add {
            users_file,
            username,
            password,
            permissions,
        } => {
            add_user(&users_file, &username, &password, permissions, ctx)?;
        }
        UserCommands::Remove {
            users_file,
            username,
        } => {
            remove_user(&users_file, &username, ctx)?;
        }
    }
    Ok(())
}

fn list_users(users_file: &Path, ctx: &CliContext) -> Result<()> {
    if !users_file.exists() {
        ctx.warn(&format!("Users file not found: {}", users_file.display()));
        ctx.info("Create it by adding a user with the 'add' command.");
        return Ok(());
    }

    let store = UserStore::from_file(users_file)?;
    let usernames = store.list_users();

    match ctx.format {
        OutputFormat::Json => {
            let data: Vec<_> = usernames
                .iter()
                .filter_map(|u| {
                    store.get_user(u).map(|user| {
                        json!({
                            "username": u,
                            "permissions": user.permissions
                        })
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&data)?
            );
        }
        _ => {
            if usernames.is_empty() {
                ctx.info("No users found.");
            } else {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL_CONDENSED);
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(vec![
                    Cell::new("Username").fg(Color::Cyan),
                    Cell::new("Permissions").fg(Color::Cyan),
                ]);

                for username in usernames {
                    if let Some(user) = store.get_user(&username) {
                        table.add_row(vec![
                            Cell::new(&username),
                            Cell::new(user.permissions.join(", ")),
                        ]);
                    }
                }

                println!("{}", table);
            }
        }
    }

    Ok(())
}

fn add_user(
    users_file: &Path,
    username: &str,
    password: &str,
    permissions: Vec<String>,
    ctx: &CliContext,
) -> Result<()> {
    // Load or create user store
    let mut store = if users_file.exists() {
        UserStore::from_file(users_file)?
    } else {
        UserStore::new()
    };

    // Check if user already exists
    if store.get_user(username).is_some() {
        return Err(streamline::StreamlineError::Config(format!(
            "User '{}' already exists. Remove it first to update.",
            username
        )));
    }

    // Create user
    let user = User::new(username.to_string(), password, permissions.clone())?;
    store.add_user(user);

    // Save to file
    store.save_to_file(users_file)?;

    match ctx.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "created",
                    "username": username,
                    "permissions": permissions
                }))?
            );
        }
        _ => {
            ctx.success(&format!("User '{}' added successfully.", username.bold()));
        }
    }
    Ok(())
}

fn remove_user(users_file: &Path, username: &str, ctx: &CliContext) -> Result<()> {
    if !users_file.exists() {
        return Err(streamline::StreamlineError::Config(format!(
            "Users file not found: {}",
            users_file.display()
        )));
    }

    if !ctx.confirm(&format!("Remove user '{}'?", username)) {
        ctx.info("Cancelled.");
        return Ok(());
    }

    let mut store = UserStore::from_file(users_file)?;

    if store.remove_user(username).is_some() {
        store.save_to_file(users_file)?;
        match ctx.format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "status": "removed",
                        "username": username
                    }))?
                );
            }
            _ => {
                ctx.success(&format!("User '{}' removed successfully.", username.bold()));
            }
        }
    } else {
        return Err(streamline::StreamlineError::Config(format!(
            "User '{}' not found.",
            username
        )));
    }

    Ok(())
}

pub(super) fn handle_acl_command(cmd: AclCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        AclCommands::List { acl_file } => {
            list_acls(&acl_file, ctx)?;
        }
        AclCommands::Add {
            acl_file,
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission,
        } => {
            add_acl(
                &acl_file,
                &resource_type,
                &resource_name,
                &pattern_type,
                &principal,
                &host,
                &operation,
                &permission,
                ctx,
            )?;
        }
        AclCommands::Remove {
            acl_file,
            resource_type,
            resource_name,
            principal,
            operation,
        } => {
            remove_acls(
                &acl_file,
                resource_type.as_deref(),
                resource_name.as_deref(),
                principal.as_deref(),
                operation.as_deref(),
                ctx,
            )?;
        }
    }
    Ok(())
}

fn list_acls(acl_file: &Path, ctx: &CliContext) -> Result<()> {
    if !acl_file.exists() {
        ctx.warn(&format!("ACL file not found: {}", acl_file.display()));
        ctx.info("Create it by adding an ACL with the 'add' command.");
        return Ok(());
    }

    let store = AclStore::from_file(acl_file)?;
    let acls = store.list_acls();

    match ctx.format {
        OutputFormat::Json => {
            let data: Vec<_> = acls
                .iter()
                .map(|acl| {
                    json!({
                        "resource_type": format!("{}", acl.resource_type),
                        "resource_name": acl.resource_name,
                        "pattern_type": format!("{}", acl.pattern_type),
                        "principal": acl.principal,
                        "host": acl.host,
                        "operation": format!("{}", acl.operation),
                        "permission": format!("{}", acl.permission)
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&data)?
            );
        }
        _ => {
            if acls.is_empty() {
                ctx.info("No ACLs found.");
            } else {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL_CONDENSED);
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(vec![
                    Cell::new("Resource").fg(Color::Cyan),
                    Cell::new("Name").fg(Color::Cyan),
                    Cell::new("Pattern").fg(Color::Cyan),
                    Cell::new("Principal").fg(Color::Cyan),
                    Cell::new("Operation").fg(Color::Cyan),
                    Cell::new("Permission").fg(Color::Cyan),
                ]);

                for acl in acls {
                    let perm_color = match format!("{}", acl.permission).as_str() {
                        "Allow" => Color::Green,
                        "Deny" => Color::Red,
                        _ => Color::White,
                    };

                    table.add_row(vec![
                        Cell::new(format!("{}", acl.resource_type)),
                        Cell::new(&acl.resource_name),
                        Cell::new(format!("{}", acl.pattern_type)),
                        Cell::new(&acl.principal),
                        Cell::new(format!("{}", acl.operation)),
                        Cell::new(format!("{}", acl.permission)).fg(perm_color),
                    ]);
                }

                println!("{}", table);
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn add_acl(
    acl_file: &Path,
    resource_type: &str,
    resource_name: &str,
    pattern_type: &str,
    principal: &str,
    host: &str,
    operation: &str,
    permission: &str,
    ctx: &CliContext,
) -> Result<()> {
    // Parse resource type
    let resource_type = ResourceType::parse(resource_type).ok_or_else(|| {
        streamline::StreamlineError::Config(format!(
            "Invalid resource type: {}. Valid types: topic, group, cluster",
            resource_type
        ))
    })?;

    // Parse pattern type
    let pattern_type = PatternType::parse(pattern_type).ok_or_else(|| {
        streamline::StreamlineError::Config(format!(
            "Invalid pattern type: {}. Valid types: literal, prefixed",
            pattern_type
        ))
    })?;

    // Parse operation
    let operation = Operation::parse(operation).ok_or_else(|| {
        streamline::StreamlineError::Config(format!(
            "Invalid operation: {}. Valid operations: read, write, create, delete, alter, describe, all",
            operation
        ))
    })?;

    // Parse permission
    let permission = Permission::parse(permission).ok_or_else(|| {
        streamline::StreamlineError::Config(format!(
            "Invalid permission: {}. Valid permissions: allow, deny",
            permission
        ))
    })?;

    // Load or create ACL store
    let mut store = if acl_file.exists() {
        AclStore::from_file(acl_file)?
    } else {
        AclStore::new()
    };

    // Create ACL
    let acl = Acl::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission,
    );

    store.add_acl(acl.clone());

    // Save to file
    store.save_to_file(acl_file)?;

    match ctx.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "created",
                    "acl": format!("{}", acl)
                }))?
            );
        }
        _ => {
            ctx.success("ACL added successfully:");
            println!("  {}", acl);
        }
    }
    Ok(())
}

fn remove_acls(
    acl_file: &Path,
    resource_type: Option<&str>,
    resource_name: Option<&str>,
    principal: Option<&str>,
    operation: Option<&str>,
    ctx: &CliContext,
) -> Result<()> {
    if !acl_file.exists() {
        return Err(streamline::StreamlineError::Config(format!(
            "ACL file not found: {}",
            acl_file.display()
        )));
    }

    if !ctx.confirm("Remove matching ACLs?") {
        ctx.info("Cancelled.");
        return Ok(());
    }

    let mut store = AclStore::from_file(acl_file)?;

    // Build filter
    let mut filter = AclFilter::new();

    if let Some(rt) = resource_type {
        if let Some(parsed) = ResourceType::parse(rt) {
            filter = filter.with_resource_type(parsed);
        } else {
            return Err(streamline::StreamlineError::Config(format!(
                "Invalid resource type: {}",
                rt
            )));
        }
    }

    if let Some(name) = resource_name {
        filter = filter.with_resource_name(name);
    }

    if let Some(p) = principal {
        filter = filter.with_principal(p);
    }

    if let Some(op) = operation {
        if let Some(parsed) = Operation::parse(op) {
            filter = filter.with_operation(parsed);
        } else {
            return Err(streamline::StreamlineError::Config(format!(
                "Invalid operation: {}",
                op
            )));
        }
    }

    let removed = store.remove_acls(&filter);

    if removed > 0 {
        store.save_to_file(acl_file)?;
        match ctx.format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "status": "removed",
                        "count": removed
                    }))?
                );
            }
            _ => {
                ctx.success(&format!("Removed {} ACL(s).", removed.to_string().green()));
            }
        }
    } else {
        ctx.info("No matching ACLs found.");
    }

    Ok(())
}
